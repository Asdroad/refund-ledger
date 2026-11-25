using Dapper;
using Npgsql;
using RefundLedger.Application;
using RefundLedger.Domain;

namespace RefundLedger.Infrastructure.Postgres;

public sealed class LedgerReader(NpgsqlDataSource db) : ILedgerReader
{
    public async Task<Refund?> FindRefundAsync(Guid id, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return (await conn.QuerySingleOrDefaultAsync<RefundRow>(new CommandDefinition(
            RefundRow.Select + " where id = @id", new { id }, cancellationToken: ct)))?.ToDomain();
    }

    /// <summary>Один запрос — один снимок: дельта не потеряется и не посчитается дважды.</summary>
    public async Task<long?> GetBalanceAsync(string accountCode, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<long?>(new CommandDefinition(
            """
            select (a.balance_minor + coalesce(
                (select sum(d.amount_minor) from balance_deltas d where d.account_id = a.id), 0))::bigint
            from accounts a where a.code = @accountCode
            """, new { accountCode }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<EntryRow>> ListEntriesAsync(
        string accountCode, long? beforeId, int limit, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var rows = await conn.QueryAsync<(long Id, string Key, long Amount, string Currency, DateTime At)>(
            new CommandDefinition(
                """
                select e.id, p.key, e.amount_minor, e.currency, e.booked_at
                from entries e join postings p on p.id = e.posting_id
                where e.account_id = (select id from accounts where code = @accountCode)
                  and e.id < @before
                order by e.id desc limit @limit
                """,
                new { accountCode, before = beforeId ?? long.MaxValue, limit }, cancellationToken: ct));
        return rows.Select(r => new EntryRow(r.Id, r.Key, r.Amount, r.Currency, r.At)).ToList();
    }
}
