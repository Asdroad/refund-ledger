using RefundLedger.Domain;

namespace RefundLedger.Application;

/// <summary>Одна транзакция БД. Без <see cref="CommitAsync"/> всё откатывается.</summary>
public interface ILedgerSession : IAsyncDisposable
{
    /// <summary>Блокирует счёт и возвращает остаток сверх порога.</summary>
    Task<long> LockAvailableAsync(AccountCode account, CancellationToken ct);

    /// <summary>false — проводка с таким ключом уже есть, ничего не записано.</summary>
    Task<bool> PostAsync(Posting posting, CancellationToken ct);

    /// <summary>Проводки одной пачкой: строгие остатки меняются на итоговую разницу.</summary>
    Task PostAllAsync(IReadOnlyList<Posting> postings, CancellationToken ct);

    Task<bool> TryAddRefundAsync(Refund refund, CancellationToken ct);
    Task<Refund?> GetRefundForUpdateAsync(Guid id, CancellationToken ct);
    Task SaveRefundAsync(Refund refund, CancellationToken ct);

    Task AddPayoutAsync(Payout payout, CancellationToken ct);
    Task<Payout?> GetPayoutForUpdateAsync(Guid id, CancellationToken ct);
    Task<IReadOnlyList<Payout>> TakeDueAsync(PayoutQueue queue, DateTimeOffset now, int limit, CancellationToken ct);
    Task SavePayoutAsync(Payout payout, CancellationToken ct);

    Task EnqueueAsync(IntegrationEvent e, CancellationToken ct);
    Task CommitAsync(CancellationToken ct);
}

public enum PayoutQueue { ToSend, ToResolve }

public interface ILedgerSessionFactory
{
    Task<ILedgerSession> BeginAsync(CancellationToken ct);
}

public interface ILedgerReader
{
    Task<Refund?> FindRefundAsync(Guid id, CancellationToken ct);
    Task<long?> GetBalanceAsync(string accountCode, CancellationToken ct);
    Task<IReadOnlyList<EntryRow>> ListEntriesAsync(string accountCode, long? beforeId, int limit, CancellationToken ct);
}

public sealed record EntryRow(long Id, string PostingKey, long AmountMinor, string Currency, DateTimeOffset BookedAt);

/// <summary>Кэш только оплаченных возвратов: они больше не меняются, сбрасывать нечего.</summary>
public interface IPaidRefundCache
{
    Task<Refund?> GetAsync(Guid id, CancellationToken ct);
    Task SetAsync(Refund refund, CancellationToken ct);
}

public abstract record PspResult
{
    public sealed record Accepted(string Reference) : PspResult;
    public sealed record Declined(string Reason) : PspResult;
    /// <summary>Провайдер точно не начал обработку: 429 или соединение не установлено.</summary>
    public sealed record NotProcessed : PspResult;
    /// <summary>Таймаут, 5xx, обрыв: деньги могли уйти.</summary>
    public sealed record Ambiguous : PspResult;
    /// <summary>Провайдер принял запрос, но ещё не закончил.</summary>
    public sealed record Processing : PspResult;
    /// <summary>Ключ провайдеру не знаком.</summary>
    public sealed record NotFound : PspResult;
}

public sealed record PspOperation(long Seq, Guid Key, long AmountMinor, string Currency, string Status);

public interface IPspClient
{
    Task<PspResult> SendAsync(Payout payout, CancellationToken ct);
    Task<PspResult> GetStatusAsync(Guid key, CancellationToken ct);
    Task<IReadOnlyList<PspOperation>> GetRegistryAsync(long afterSeq, int limit, CancellationToken ct);
}

/// <param name="Version">Версия возврата. Потребитель отбрасывает события старше известной ему.</param>
public sealed record IntegrationEvent(
    Guid EventId, string Type, Guid RefundId, int Version, long AmountMinor, string Currency,
    DateTimeOffset OccurredAt)
{
    public static IntegrationEvent For(string type, Refund r, DateTimeOffset now) =>
        new(Guid.NewGuid(), type, r.Id, r.Version, r.Amount.Minor, r.Amount.Currency.Code, now);
}

public sealed class ConflictException(string message) : Exception(message);

public sealed class NotFoundException(string message) : Exception(message);

public sealed class InsufficientFundsException(string account)
    : Exception($"На счёте {account} недостаточно средств");

public sealed class PayoutOptions
{
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan PspTimeout { get; set; } = TimeSpan.FromSeconds(5);
    /// <summary>
    /// Сколько ждать, прежде чем верить ответу «ключ не найден»:
    /// исходный запрос ещё может ехать до провайдера.
    /// </summary>
    public TimeSpan NotFoundGrace { get; set; } = TimeSpan.FromSeconds(60);
    public int BatchSize { get; set; } = 20;
}
