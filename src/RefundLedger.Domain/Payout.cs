namespace RefundLedger.Domain;

public enum PayoutStatus
{
    Pending = 1,
    Sending = 2,
    /// <summary>Запрос ушёл, ответа нет. Это не отказ: деньги могли уйти.</summary>
    Unknown = 3,
    Succeeded = 4,
    Rejected = 5
}

public sealed class Payout
{
    /// <summary>Он же ключ идемпотентности у провайдера: повтор не создаст второй перевод.</summary>
    public Guid Id { get; }
    public Guid RefundId { get; }
    public Money Amount { get; }
    public PayoutStatus Status { get; private set; }
    public int Attempts { get; private set; }
    public DateTimeOffset NextAttemptAt { get; private set; }
    public Guid? LeaseToken { get; private set; }
    public DateTimeOffset? LeaseUntil { get; private set; }
    public string? PspReference { get; private set; }
    public string? RejectReason { get; private set; }
    public string? TraceParent { get; }
    public DateTimeOffset? SentAt { get; private set; }

    public Payout(Guid id, Guid refundId, Money amount, PayoutStatus status, int attempts,
        DateTimeOffset nextAttemptAt, Guid? leaseToken, DateTimeOffset? leaseUntil,
        string? pspReference, string? rejectReason, string? traceParent,
        DateTimeOffset? sentAt)
    {
        (Id, RefundId, Amount, Status, Attempts, NextAttemptAt) =
            (id, refundId, amount, status, attempts, nextAttemptAt);
        (LeaseToken, LeaseUntil, PspReference, RejectReason, TraceParent, SentAt) =
            (leaseToken, leaseUntil, pspReference, rejectReason, traceParent, sentAt);
    }

    public static Payout Create(Guid id, Refund refund, DateTimeOffset now, string? traceParent) =>
        new(id, refund.Id, refund.Amount, PayoutStatus.Pending, 0, now, null, null, null, null, traceParent, null);

    public bool IsTerminal => Status is PayoutStatus.Succeeded or PayoutStatus.Rejected;

    /// <summary>Аренда истекла, значит, отправитель умер на полпути и итог неизвестен.</summary>
    public void Lease(DateTimeOffset now, TimeSpan duration)
    {
        switch (Status)
        {
            case PayoutStatus.Pending when NextAttemptAt <= now:
                Status = PayoutStatus.Sending;
                Attempts++;
                SentAt = now;
                break;
            case PayoutStatus.Sending when LeaseUntil < now:
            case PayoutStatus.Unknown when NextAttemptAt <= now && (LeaseUntil is null || LeaseUntil < now):
                Status = PayoutStatus.Unknown;
                break;
            default:
                throw new InvalidTransitionException($"Выплату {Id} нельзя взять в работу из {Status}");
        }

        LeaseToken = Guid.NewGuid();
        LeaseUntil = now + duration;
    }

    public bool HoldsLease(Guid token) => !IsTerminal && LeaseToken == token;

    public void Succeed(string pspReference)
    {
        EnsureInFlight(PayoutStatus.Succeeded);
        PspReference = pspReference;
        Finish(PayoutStatus.Succeeded);
    }

    public void Reject(string reason)
    {
        EnsureInFlight(PayoutStatus.Rejected);
        RejectReason = reason;
        Finish(PayoutStatus.Rejected);
    }

    public void MarkUnknown(DateTimeOffset checkAt)
    {
        EnsureInFlight(PayoutStatus.Unknown);
        Status = PayoutStatus.Unknown;
        Release(checkAt);
    }

    /// <summary>Провайдер точно не принял запрос. Ключ при повторе прежний.</summary>
    public void Resend(DateTimeOffset at)
    {
        EnsureInFlight(PayoutStatus.Pending);
        Status = PayoutStatus.Pending;
        Release(at);
    }

    private void EnsureInFlight(PayoutStatus to)
    {
        if (Status is not (PayoutStatus.Sending or PayoutStatus.Unknown))
            throw new InvalidTransitionException($"Выплата {Id}: переход {Status} → {to} запрещён");
    }

    private void Finish(PayoutStatus status)
    {
        Status = status;
        LeaseToken = null;
        LeaseUntil = null;
    }

    private void Release(DateTimeOffset nextAt)
    {
        NextAttemptAt = nextAt;
        LeaseToken = null;
        LeaseUntil = null;
    }
}

public static class Backoff
{
    /// <summary>Джиттер нужен, чтобы после сбоя провайдера повторы не пришли залпом.</summary>
    public static TimeSpan For(int attempt, Random random, TimeSpan? max = null)
    {
        var cap = (max ?? TimeSpan.FromMinutes(5)).TotalSeconds;
        var ceiling = Math.Min(cap, Math.Pow(2, Math.Clamp(attempt, 0, 20)));
        return TimeSpan.FromSeconds(ceiling * (0.5 + random.NextDouble() / 2));
    }
}
