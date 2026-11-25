using System.Diagnostics;
using System.Text.Json;
using Dapper;
using Npgsql;
using RefundLedger.Application;
using RefundLedger.Domain;

namespace RefundLedger.Infrastructure.Postgres;

public sealed class LedgerOptions
{
    /// <summary>До какого минуса продавец может уйти на возвратах.</summary>
    public long SellerCreditLimitMinor { get; set; } = 0;
}

public sealed class LedgerSessionFactory(NpgsqlDataSource db, AccountDirectory accounts) : ILedgerSessionFactory
{
    public async Task<ILedgerSession> BeginAsync(CancellationToken ct)
    {
        var conn = await db.OpenConnectionAsync(ct);
        try
        {
            return new LedgerSession(conn, await conn.BeginTransactionAsync(ct), accounts);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }
}

internal sealed class LedgerSession(NpgsqlConnection conn, NpgsqlTransaction tx, AccountDirectory accounts)
    : ILedgerSession
{
    public async Task<long> LockAvailableAsync(AccountCode account, CancellationToken ct)
    {
        var acc = await accounts.ResolveAsync(account, ct);
        return await conn.ExecuteScalarAsync<long>(Cmd(
            "select balance_minor - coalesce(floor_minor, 0) from accounts where id = @Id for no key update",
            new { acc.Id }, ct));
    }

    public async Task<bool> PostAsync(Posting posting, CancellationToken ct)
    {
        var acc = await ResolveAsync([posting], ct);
        if (await InsertPostingAsync(posting, acc, ct) is null) return false;
        await ApplyBalancesAsync([posting], acc, ct);
        return true;
    }

    public async Task PostAllAsync(IReadOnlyList<Posting> postings, CancellationToken ct)
    {
        var acc = await ResolveAsync(postings, ct);
        foreach (var p in postings)
            if (await InsertPostingAsync(p, acc, ct) is null)
                throw new ConflictException($"Проводка {p.Key} уже записана");
        await ApplyBalancesAsync(postings, acc, ct);
    }

    private async Task<Dictionary<AccountCode, AccountRef>> ResolveAsync(
        IEnumerable<Posting> postings, CancellationToken ct)
    {
        var result = new Dictionary<AccountCode, AccountRef>();
        foreach (var code in postings.SelectMany(p => p.Lines).Select(l => l.Account).Distinct())
            result[code] = await accounts.ResolveAsync(code, ct);
        return result;
    }

    private async Task<long?> InsertPostingAsync(
        Posting p, Dictionary<AccountCode, AccountRef> acc, CancellationToken ct)
    {
        var id = await conn.ExecuteScalarAsync<long?>(Cmd(
            "insert into postings (key, currency) values (@Key, @Currency) on conflict (key) do nothing returning id",
            new { p.Key, Currency = p.Currency.Code }, ct));
        if (id is null) return null;

        await conn.ExecuteAsync(Cmd(
            """
            insert into entries (posting_id, account_id, currency, amount_minor)
            select @id, a, @Currency, m from unnest(@Accounts, @Amounts) as t(a, m)
            """,
            new
            {
                id,
                Currency = p.Currency.Code,
                Accounts = p.Lines.Select(l => acc[l.Account].Id).ToArray(),
                Amounts = p.Lines.Select(l => l.Amount.Minor).ToArray()
            }, ct));
        return id;
    }

    private async Task ApplyBalancesAsync(
        IEnumerable<Posting> postings, Dictionary<AccountCode, AccountRef> acc, CancellationToken ct)
    {
        // Сортировка по id — единый порядок блокировок, он исключает взаимные.
        // Счёт с нулевой разницей не блокируется вовсе: так проходит транзитный счёт покупателя.
        var net = postings.SelectMany(p => p.Lines)
            .GroupBy(l => l.Account)
            .Select(g => (Code: g.Key, Ref: acc[g.Key], Sum: g.Sum(l => l.Amount.Minor)))
            .Where(x => x.Sum != 0)
            .OrderBy(x => x.Ref.Id)
            .ToList();

        foreach (var (code, account, sum) in net)
        {
            if (account.Strict)
            {
                var rows = await conn.ExecuteAsync(Cmd(
                    """
                    update accounts set balance_minor = balance_minor + @sum
                    where id = @Id and (floor_minor is null or balance_minor + @sum >= floor_minor)
                    """,
                    new { account.Id, sum }, ct));
                if (rows == 0) throw new InsufficientFundsException(code.ToString());
            }
            else
            {
                await conn.ExecuteAsync(Cmd(
                    "insert into balance_deltas (account_id, amount_minor) values (@Id, @sum)",
                    new { account.Id, sum }, ct));
            }
        }
    }

    public async Task<bool> TryAddRefundAsync(Refund r, CancellationToken ct) =>
        await conn.ExecuteAsync(Cmd(
            """
            insert into refunds (id, buyer_id, seller_id, amount_minor, seller_minor, currency, status, version)
            values (@Id, @BuyerId, @SellerId, @Amount, @Seller, @Currency, @Status, @Version)
            on conflict (id) do nothing
            """,
            new
            {
                r.Id, r.BuyerId, r.SellerId, Amount = r.Amount.Minor, Seller = r.SellerPart.Minor,
                Currency = r.Amount.Currency.Code, Status = (short)r.Status, r.Version
            }, ct)) == 1;

    public async Task<Refund?> GetRefundForUpdateAsync(Guid id, CancellationToken ct) =>
        (await conn.QuerySingleOrDefaultAsync<RefundRow>(Cmd(
            RefundRow.Select + " where id = @id for update", new { id }, ct)))?.ToDomain();

    public Task SaveRefundAsync(Refund r, CancellationToken ct) => conn.ExecuteAsync(Cmd(
        "update refunds set status = @Status, version = @Version where id = @Id",
        new { r.Id, Status = (short)r.Status, r.Version }, ct));

    public Task AddPayoutAsync(Payout p, CancellationToken ct) => conn.ExecuteAsync(Cmd(
        """
        insert into payouts (id, refund_id, amount_minor, currency, status, attempts, next_attempt_at, trace_parent)
        values (@Id, @RefundId, @Amount, @Currency, @Status, @Attempts, @NextAttemptAt, @TraceParent)
        """,
        new
        {
            p.Id, p.RefundId, Amount = p.Amount.Minor, Currency = p.Amount.Currency.Code,
            Status = (short)p.Status, p.Attempts, p.NextAttemptAt, p.TraceParent
        }, ct));

    public async Task<Payout?> GetPayoutForUpdateAsync(Guid id, CancellationToken ct) =>
        (await conn.QuerySingleOrDefaultAsync<PayoutRow>(Cmd(
            PayoutRow.Select + " where id = @id for update", new { id }, ct)))?.ToDomain();

    public async Task<IReadOnlyList<Payout>> TakeDueAsync(
        PayoutQueue queue, DateTimeOffset now, int limit, CancellationToken ct)
    {
        var filter = queue == PayoutQueue.ToSend
            ? "status = 1 and next_attempt_at <= @now"
            : """
              status in (2, 3)
              and (lease_until is null or lease_until < @now)
              and (status = 2 or next_attempt_at <= @now)
              """;
        var rows = await conn.QueryAsync<PayoutRow>(Cmd(
            $"{PayoutRow.Select} where {filter} order by next_attempt_at limit @limit for update skip locked",
            new { now, limit }, ct));
        return rows.Select(r => r.ToDomain()).ToList();
    }

    public Task SavePayoutAsync(Payout p, CancellationToken ct) => conn.ExecuteAsync(Cmd(
        """
        update payouts set status = @Status, attempts = @Attempts, next_attempt_at = @NextAttemptAt,
            lease_token = @LeaseToken, lease_until = @LeaseUntil,
            psp_reference = @PspReference, reject_reason = @RejectReason, sent_at = @SentAt
        where id = @Id
        """,
        new
        {
            p.Id, Status = (short)p.Status, p.Attempts, p.NextAttemptAt, p.LeaseToken, p.LeaseUntil,
            p.PspReference, p.RejectReason, p.SentAt
        }, ct));

    public Task EnqueueAsync(IntegrationEvent e, CancellationToken ct) => conn.ExecuteAsync(Cmd(
        """
        insert into outbox (message_key, type, payload, trace_parent)
        values (@Key, @Type, @Payload::jsonb, @Trace)
        """,
        new
        {
            Key = e.RefundId.ToString(), e.Type,
            Payload = JsonSerializer.Serialize(e, JsonSerializerOptions.Web), Trace = Activity.Current?.Id
        }, ct));

    public Task CommitAsync(CancellationToken ct) => tx.CommitAsync(ct);

    public async ValueTask DisposeAsync()
    {
        await tx.DisposeAsync();
        await conn.DisposeAsync();
    }

    private CommandDefinition Cmd(string sql, object args, CancellationToken ct) =>
        new(sql, args, tx, cancellationToken: ct);
}

internal sealed class RefundRow
{
    public const string Select =
        """
        select id as Id, buyer_id as BuyerId, seller_id as SellerId, amount_minor as AmountMinor,
               seller_minor as SellerMinor, currency as Currency, status as Status, version as Version
        from refunds
        """;

    public Guid Id { get; init; }
    public long BuyerId { get; init; }
    public long SellerId { get; init; }
    public long AmountMinor { get; init; }
    public long SellerMinor { get; init; }
    public string Currency { get; init; } = "";
    public short Status { get; init; }
    public int Version { get; init; }

    public Refund ToDomain()
    {
        var c = new Currency(Currency);
        return new(Id, BuyerId, SellerId, new(AmountMinor, c), new(SellerMinor, c), (RefundStatus)Status, Version);
    }
}

internal sealed class PayoutRow
{
    public const string Select =
        """
        select id as Id, refund_id as RefundId, amount_minor as AmountMinor, currency as Currency,
               status as Status, attempts as Attempts, next_attempt_at as NextAttemptAt,
               lease_token as LeaseToken, lease_until as LeaseUntil, psp_reference as PspReference,
               reject_reason as RejectReason, trace_parent as TraceParent, sent_at as SentAt
        from payouts
        """;

    public Guid Id { get; init; }
    public Guid RefundId { get; init; }
    public long AmountMinor { get; init; }
    public string Currency { get; init; } = "";
    public short Status { get; init; }
    public int Attempts { get; init; }
    public DateTime NextAttemptAt { get; init; }
    public Guid? LeaseToken { get; init; }
    public DateTime? LeaseUntil { get; init; }
    public string? PspReference { get; init; }
    public string? RejectReason { get; init; }
    public string? TraceParent { get; init; }
    public DateTime? SentAt { get; init; }

    public Payout ToDomain() => new(Id, RefundId, new(AmountMinor, new(Currency)), (PayoutStatus)Status,
        Attempts, NextAttemptAt, LeaseToken, LeaseUntil, PspReference, RejectReason, TraceParent, SentAt);
}
