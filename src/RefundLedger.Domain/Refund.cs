namespace RefundLedger.Domain;

public enum RefundStatus { PayoutInProgress = 1, Paid = 2, PayoutRejected = 3 }

public sealed class Refund
{
    public Guid Id { get; }
    public long BuyerId { get; }
    public long SellerId { get; }
    public Money Amount { get; }
    public Money SellerPart { get; }
    public Money PlatformPart => Amount - SellerPart;
    public RefundStatus Status { get; private set; }

    /// <summary>Уходит в события: по нему потребитель отбрасывает устаревшие.</summary>
    public int Version { get; private set; }

    public Refund(Guid id, long buyerId, long sellerId, Money amount, Money sellerPart,
        RefundStatus status, int version)
    {
        (Id, BuyerId, SellerId, Amount, SellerPart, Status, Version) =
            (id, buyerId, sellerId, amount, sellerPart, status, version);
    }

    /// <param name="sellerAvailable">
    /// Сколько можно удержать с продавца. Нехватку берёт на себя маркетплейс:
    /// покупатель не должен ждать, пока у продавца появятся деньги.
    /// </param>
    public static Refund Register(Guid id, long buyerId, long sellerId, Money amount, long sellerAvailable)
    {
        if (id == Guid.Empty) throw new DomainException("Пустой идентификатор возврата");
        if (buyerId <= 0 || sellerId <= 0) throw new DomainException("Некорректный покупатель или продавец");
        if (amount.Minor <= 0) throw new DomainException("Сумма должна быть положительной");

        var sellerPart = amount with { Minor = Math.Clamp(sellerAvailable, 0, amount.Minor) };
        return new Refund(id, buyerId, sellerId, amount, sellerPart, RefundStatus.PayoutInProgress, 1);
    }

    public bool SameRequest(long buyerId, long sellerId, Money amount) =>
        BuyerId == buyerId && SellerId == sellerId && Amount == amount;

    public void MarkPaid() => Move(RefundStatus.PayoutInProgress, RefundStatus.Paid);
    public void MarkRejected() => Move(RefundStatus.PayoutInProgress, RefundStatus.PayoutRejected);
    public void Retry() => Move(RefundStatus.PayoutRejected, RefundStatus.PayoutInProgress);

    private void Move(RefundStatus from, RefundStatus to)
    {
        if (Status != from) throw new InvalidTransitionException($"Возврат {Id}: переход {Status} → {to} запрещён");
        Status = to;
        Version++;
    }
}
