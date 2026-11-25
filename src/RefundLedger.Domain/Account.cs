namespace RefundLedger.Domain;

public enum AccountKind
{
    /// <summary>Сколько маркетплейс должен продавцу. Ниже нуля — в пределах лимита.</summary>
    Seller,
    /// <summary>Сколько маркетплейс должен покупателю. Ниже нуля быть не может.</summary>
    Buyer,
    /// <summary>Выплата отправлена, итог ещё неизвестен.</summary>
    InTransit,
    /// <summary>Деньги, ушедшие через платёжного провайдера.</summary>
    PspClearing,
    /// <summary>Расход маркетплейса: продавцу не хватило остатка.</summary>
    Compensation,
    /// <summary>Источник начислений продавцам.</summary>
    SalesClearing
}

public enum BalanceMode
{
    /// <summary>Остаток меняется в той же транзакции: у счёта есть порог.</summary>
    Strict,
    /// <summary>Остаток собирается из дельт в фоне: порога нет, а пишут в счёт все.</summary>
    Delta
}

/// <summary>Код счёта вида <c>seller:42:RUB</c> или <c>in_transit:RUB</c>.</summary>
public sealed record AccountCode
{
    public AccountKind Kind { get; }
    public long? OwnerId { get; }
    public Currency Currency { get; }

    private AccountCode(AccountKind kind, long? ownerId, Currency currency) =>
        (Kind, OwnerId, Currency) = (kind, ownerId, currency);

    public static AccountCode Seller(long id, Currency c) => new(AccountKind.Seller, id, c);
    public static AccountCode Buyer(long id, Currency c) => new(AccountKind.Buyer, id, c);
    public static AccountCode InTransit(Currency c) => new(AccountKind.InTransit, null, c);
    public static AccountCode PspClearing(Currency c) => new(AccountKind.PspClearing, null, c);
    public static AccountCode Compensation(Currency c) => new(AccountKind.Compensation, null, c);
    public static AccountCode SalesClearing(Currency c) => new(AccountKind.SalesClearing, null, c);

    public BalanceMode Mode => OwnerId is null ? BalanceMode.Delta : BalanceMode.Strict;

    public override string ToString()
    {
        var name = Kind switch
        {
            AccountKind.Seller => "seller",
            AccountKind.Buyer => "buyer",
            AccountKind.InTransit => "in_transit",
            AccountKind.PspClearing => "psp_clearing",
            AccountKind.Compensation => "compensation",
            AccountKind.SalesClearing => "sales_clearing",
            _ => throw new InvalidOperationException()
        };
        return OwnerId is { } id ? $"{name}:{id}:{Currency}" : $"{name}:{Currency}";
    }
}
