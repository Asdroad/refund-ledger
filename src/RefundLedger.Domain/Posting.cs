namespace RefundLedger.Domain;

public sealed record PostingLine(AccountCode Account, Money Amount);

/// <summary>Проводка. Несбалансированную создать нельзя: сумма строк всегда ноль.</summary>
public sealed record Posting
{
    /// <summary>Ключ идемпотентности: второй раз та же проводка не запишется.</summary>
    public string Key { get; }
    public Currency Currency { get; }
    public IReadOnlyList<PostingLine> Lines { get; }

    private Posting(string key, Currency currency, IReadOnlyList<PostingLine> lines) =>
        (Key, Currency, Lines) = (key, currency, lines);

    public static Posting Create(string key, params IReadOnlyList<PostingLine> lines)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new DomainException("У проводки должен быть ключ");
        if (lines.Count < 2)
            throw new DomainException("В проводке должно быть не меньше двух строк");

        var currency = lines[0].Amount.Currency;
        long sum = 0;
        foreach (var line in lines)
        {
            if (line.Amount.Minor == 0)
                throw new DomainException("Нулевая строка проводки");
            if (line.Amount.Currency != currency || line.Account.Currency != currency)
                throw new DomainException("Все строки и счета проводки должны быть в одной валюте");
            sum = checked(sum + line.Amount.Minor);
        }

        if (sum != 0)
            throw new DomainException($"Проводка не сбалансирована: {sum}");
        if (lines.Select(l => l.Account).Distinct().Count() != lines.Count)
            throw new DomainException("Счёт встречается в проводке дважды");

        return new Posting(key, currency, lines);
    }
}

public static class Postings
{
    public static Posting SaleCredited(string operationId, long sellerId, Money amount) =>
        Posting.Create($"sale:{operationId}",
            new(AccountCode.SalesClearing(amount.Currency), -amount),
            new(AccountCode.Seller(sellerId, amount.Currency), amount));

    public static Posting RefundAccrued(Refund r)
    {
        var c = r.Amount.Currency;
        var lines = new List<PostingLine>(3);
        if (r.SellerPart.Minor > 0) lines.Add(new(AccountCode.Seller(r.SellerId, c), -r.SellerPart));
        if (r.PlatformPart.Minor > 0) lines.Add(new(AccountCode.Compensation(c), -r.PlatformPart));
        lines.Add(new(AccountCode.Buyer(r.BuyerId, c), r.Amount));
        return Posting.Create($"refund:{r.Id}:accrued", lines);
    }

    public static Posting PayoutReserved(Refund r, Payout p) => Create(p, "reserved",
        from: AccountCode.Buyer(r.BuyerId, p.Amount.Currency),
        to: AccountCode.InTransit(p.Amount.Currency));

    public static Posting PayoutSettled(Payout p) => Create(p, "settled",
        from: AccountCode.InTransit(p.Amount.Currency),
        to: AccountCode.PspClearing(p.Amount.Currency));

    public static Posting PayoutReturned(Refund r, Payout p) => Create(p, "returned",
        from: AccountCode.InTransit(p.Amount.Currency),
        to: AccountCode.Buyer(r.BuyerId, p.Amount.Currency));

    private static Posting Create(Payout p, string step, AccountCode from, AccountCode to) =>
        Posting.Create($"payout:{p.Id}:{step}", new(from, -p.Amount), new(to, p.Amount));
}
