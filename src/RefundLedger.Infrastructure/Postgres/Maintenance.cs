using Dapper;
using Npgsql;

namespace RefundLedger.Infrastructure.Postgres;

public sealed class Maintenance(NpgsqlDataSource db)
{
    private const long RollUpLock = 7_203_004;

    /// <summary>Сворачивает дельты в остаток. DELETE видит только закоммиченное, пропусков нет.</summary>
    public async Task<int> RollUpAsync(int limit, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Порядок обновления счетов внутри запроса не задать, и две свёртки
        // взаимно блокировались. Поэтому сворачивает один экземпляр.
        if (!await conn.ExecuteScalarAsync<bool>(new CommandDefinition(
                "select pg_try_advisory_xact_lock(@RollUpLock)", new { RollUpLock }, tx, cancellationToken: ct)))
            return 0;

        var count = await conn.ExecuteScalarAsync<int>(new CommandDefinition(
            """
            with taken as (
                delete from balance_deltas where id in (
                    select id from balance_deltas order by id limit @limit)
                returning account_id, amount_minor
            ), sums as (
                select account_id, sum(amount_minor)::bigint as total from taken group by account_id
            ), upd as (
                update accounts a set balance_minor = a.balance_minor + s.total
                from sums s where a.id = s.account_id
            )
            select count(*)::int from taken
            """, new { limit }, tx, cancellationToken: ct));
        await tx.CommitAsync(ct);
        return count;
    }

    public async Task EnsurePartitionsAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            """
            begin;
            set local timezone = 'UTC';
            select pg_advisory_xact_lock(7203002);
            select ensure_entries_partition((date_trunc('month', now()) + make_interval(months => m))::date)
            from generate_series(0, 2) m;
            commit;
            """, cancellationToken: ct));
    }

    /// <summary>Сверка леджера с самим собой. Обе цифры всегда должны быть нулём.</summary>
    public async Task<(long Imbalance, long DriftedAccounts)> SelfCheckAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<(long, long)>(new CommandDefinition(
            """
            select
              (select coalesce(sum(amount_minor), 0)::bigint from entries),
              (select count(*) from accounts a
               where a.balance_minor
                   + coalesce((select sum(d.amount_minor) from balance_deltas d where d.account_id = a.id), 0)
                  <> coalesce((select sum(e.amount_minor) from entries e where e.account_id = a.id), 0))
            """, commandTimeout: 300, cancellationToken: ct));
    }

    public async Task<LedgerStats> GetStatsAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.QuerySingleAsync<LedgerStats>(new CommandDefinition(
            """
            select
              (select count(*) from outbox) as Outbox,
              (select coalesce(extract(epoch from now() - min(created_at)), 0)::float8 from outbox) as OutboxAgeSeconds,
              (select count(*) from balance_deltas) as Deltas,
              (select count(*) from payouts where status = 1) as PayoutsPending,
              (select count(*) from payouts where status in (2, 3)) as PayoutsInFlight,
              (select coalesce(extract(epoch from now() - min(sent_at)), 0)::float8
                 from payouts where status = 3) as OldestUnknownSeconds,
              (select count(*) from reconciliation_issues) as ReconciliationIssues
            """, cancellationToken: ct));
    }
}

public sealed record LedgerStats(
    long Outbox, double OutboxAgeSeconds, long Deltas, long PayoutsPending, long PayoutsInFlight,
    double OldestUnknownSeconds, long ReconciliationIssues);
