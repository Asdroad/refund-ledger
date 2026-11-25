using System.Diagnostics;
using RefundLedger.Domain;

namespace RefundLedger.Application;

public sealed class RegisterRefund(ILedgerSessionFactory sessions, ILedgerReader reader, TimeProvider clock)
{
    public sealed record Command(Guid RefundId, long BuyerId, long SellerId, Money Amount);

    public async Task<Refund> ExecuteAsync(Command cmd, CancellationToken ct)
    {
        if (await reader.FindRefundAsync(cmd.RefundId, ct) is { } known)
            return EnsureSame(known, cmd);

        await using (var tx = await sessions.BeginAsync(ct))
        {
            // Доля продавца зависит от остатка, поэтому считается под блокировкой:
            // иначе два возврата разделят один и тот же остаток.
            var available = await tx.LockAvailableAsync(
                AccountCode.Seller(cmd.SellerId, cmd.Amount.Currency), ct);
            var refund = Refund.Register(cmd.RefundId, cmd.BuyerId, cmd.SellerId, cmd.Amount, available);

            if (await tx.TryAddRefundAsync(refund, ct))
            {
                var now = clock.GetUtcNow();
                var payout = Payout.Create(Guid.NewGuid(), refund, now, Activity.Current?.Id);
                await tx.AddPayoutAsync(payout, ct);
                await tx.PostAllAsync([Postings.RefundAccrued(refund), Postings.PayoutReserved(refund, payout)], ct);
                await tx.EnqueueAsync(IntegrationEvent.For("refund.registered", refund, now), ct);
                await tx.CommitAsync(ct);
                return refund;
            }
        }

        // Параллельный дубль успел раньше: отвечаем тем, что записал он.
        var winner = await reader.FindRefundAsync(cmd.RefundId, ct)
                     ?? throw new InvalidOperationException($"Возврат {cmd.RefundId} исчез");
        return EnsureSame(winner, cmd);
    }

    private static Refund EnsureSame(Refund r, Command cmd) =>
        r.SameRequest(cmd.BuyerId, cmd.SellerId, cmd.Amount)
            ? r
            : throw new ConflictException($"Возврат {r.Id} уже зарегистрирован с другими данными");
}

public sealed class CreditSeller(ILedgerSessionFactory sessions)
{
    /// <returns>true, если операция уже была проведена раньше.</returns>
    public async Task<bool> ExecuteAsync(string operationId, long sellerId, Money amount, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(operationId)) throw new DomainException("Пустой идентификатор операции");
        if (sellerId <= 0) throw new DomainException("Некорректный продавец");

        await using var tx = await sessions.BeginAsync(ct);
        var posted = await tx.PostAsync(Postings.SaleCredited(operationId, sellerId, amount), ct);
        if (posted) await tx.CommitAsync(ct);
        return !posted;
    }
}

public sealed class RetryPayout(ILedgerSessionFactory sessions, TimeProvider clock)
{
    public async Task<Refund> ExecuteAsync(Guid refundId, CancellationToken ct)
    {
        await using var tx = await sessions.BeginAsync(ct);
        var refund = await tx.GetRefundForUpdateAsync(refundId, ct)
                     ?? throw new NotFoundException($"Возврат {refundId} не найден");
        refund.Retry();

        var now = clock.GetUtcNow();
        var payout = Payout.Create(Guid.NewGuid(), refund, now, Activity.Current?.Id);
        await tx.AddPayoutAsync(payout, ct);
        await tx.PostAllAsync([Postings.PayoutReserved(refund, payout)], ct);
        await tx.SaveRefundAsync(refund, ct);
        await tx.EnqueueAsync(IntegrationEvent.For("payout.retried", refund, now), ct);
        await tx.CommitAsync(ct);
        return refund;
    }
}

public sealed class GetRefund(ILedgerReader reader, IPaidRefundCache cache)
{
    public async Task<Refund> ExecuteAsync(Guid id, CancellationToken ct)
    {
        if (await cache.GetAsync(id, ct) is { } cached) return cached;

        var refund = await reader.FindRefundAsync(id, ct) ?? throw new NotFoundException($"Возврат {id} не найден");
        if (refund.Status == RefundStatus.Paid) await cache.SetAsync(refund, ct);
        return refund;
    }
}

/// <summary>
/// Берёт выплаты в аренду, зовёт провайдера вне транзакции и записывает итог.
/// Держать транзакцию открытой на время внешнего вызова нельзя: при медленном
/// провайдере кончатся соединения.
/// </summary>
public sealed class ProcessPayouts(
    ILedgerSessionFactory sessions, IPspClient psp, TimeProvider clock, PayoutOptions options)
{
    public static readonly ActivitySource Source = new("RefundLedger.Payouts");

    public async Task<int> ExecuteAsync(PayoutQueue queue, CancellationToken ct)
    {
        IReadOnlyList<Payout> batch;
        await using (var tx = await sessions.BeginAsync(ct))
        {
            var now = clock.GetUtcNow();
            batch = await tx.TakeDueAsync(queue, now, options.BatchSize, ct);
            foreach (var p in batch)
            {
                p.Lease(now, options.Lease);
                await tx.SavePayoutAsync(p, ct);
            }

            await tx.CommitAsync(ct);
        }

        await Task.WhenAll(batch.Select(p => HandleAsync(queue, p, ct)));
        return batch.Count;
    }

    private async Task HandleAsync(PayoutQueue queue, Payout leased, CancellationToken ct)
    {
        ActivityContext.TryParse(leased.TraceParent, null, out var parent);
        using var activity = Source.StartActivity($"payout {queue}", ActivityKind.Internal, parent);
        activity?.SetTag("payout.id", leased.Id);

        var result = queue == PayoutQueue.ToSend
            ? await psp.SendAsync(leased, ct)
            : await psp.GetStatusAsync(leased.Id, ct);
        activity?.SetTag("psp.result", result.GetType().Name);
        await ApplyAsync(leased, result, ct);
    }

    public async Task ApplyAsync(Payout leased, PspResult result, CancellationToken ct)
    {
        await using var tx = await sessions.BeginAsync(ct);

        // Порядок блокировок везде один: возврат, потом выплата.
        var refund = await tx.GetRefundForUpdateAsync(leased.RefundId, ct)
                     ?? throw new InvalidOperationException($"Нет возврата {leased.RefundId}");
        var payout = await tx.GetPayoutForUpdateAsync(leased.Id, ct)
                     ?? throw new InvalidOperationException($"Нет выплаты {leased.Id}");

        // Аренду перехватили: итог запишет новый владелец.
        if (!payout.HoldsLease(leased.LeaseToken!.Value)) return;

        var now = clock.GetUtcNow();
        var next = now + Backoff.For(payout.Attempts, Random.Shared);
        switch (result)
        {
            case PspResult.Accepted a:
                payout.Succeed(a.Reference);
                refund.MarkPaid();
                await tx.PostAllAsync([Postings.PayoutSettled(payout)], ct);
                await tx.SaveRefundAsync(refund, ct);
                await tx.EnqueueAsync(IntegrationEvent.For("payout.succeeded", refund, now), ct);
                break;

            case PspResult.Declined d:
                payout.Reject(d.Reason);
                refund.MarkRejected();
                await tx.PostAllAsync([Postings.PayoutReturned(refund, payout)], ct);
                await tx.SaveRefundAsync(refund, ct);
                await tx.EnqueueAsync(IntegrationEvent.For("payout.rejected", refund, now), ct);
                break;

            case PspResult.NotProcessed when payout.Status == PayoutStatus.Sending:
                payout.Resend(next);
                break;

            case PspResult.NotFound when now - payout.SentAt >= options.NotFoundGrace:
                payout.Resend(now);
                break;

            default:
                payout.MarkUnknown(next);
                break;
        }

        await tx.SavePayoutAsync(payout, ct);
        await tx.CommitAsync(ct);
    }
}
