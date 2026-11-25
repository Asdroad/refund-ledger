using RefundLedger.Domain;

namespace RefundLedger.UnitTests;

public class PostingTests
{
    private static readonly Currency Rub = new("RUB");
    private static Money M(long v) => new(v, Rub);

    [Fact]
    public void Несбалансированная_проводка_невозможна() =>
        Assert.Throws<DomainException>(() => Posting.Create("k",
            new PostingLine(AccountCode.Seller(1, Rub), M(-100)),
            new PostingLine(AccountCode.Buyer(1, Rub), M(99))));

    [Fact]
    public void Разные_валюты_запрещены() =>
        Assert.Throws<DomainException>(() => Posting.Create("k",
            new PostingLine(AccountCode.Seller(1, Rub), M(-100)),
            new PostingLine(AccountCode.Buyer(1, new("USD")), new Money(100, new("USD")))));

    [Fact]
    public void Валюта_счёта_и_строки_должны_совпадать() =>
        Assert.Throws<DomainException>(() => Posting.Create("k",
            new PostingLine(AccountCode.Seller(1, new("USD")), M(-100)),
            new PostingLine(AccountCode.Buyer(1, Rub), M(100))));

    [Fact]
    public void Нулевая_строка_запрещена() =>
        Assert.Throws<DomainException>(() => Posting.Create("k",
            new PostingLine(AccountCode.Seller(1, Rub), M(0)),
            new PostingLine(AccountCode.Buyer(1, Rub), M(0))));

    [Fact]
    public void Переполнение_не_маскируется() =>
        Assert.Throws<OverflowException>(() => M(long.MaxValue) + M(1));

    [Theory]
    [InlineData(1000, 1000, 0)]
    [InlineData(300, 300, 700)]
    [InlineData(0, 0, 1000)]
    [InlineData(-50, 0, 1000)]
    [InlineData(5000, 1000, 0)]
    public void Нехватку_у_продавца_покрывает_маркетплейс(long available, long seller, long platform)
    {
        var r = Refund.Register(Guid.NewGuid(), 1, 2, M(1000), available);

        Assert.Equal(seller, r.SellerPart.Minor);
        Assert.Equal(platform, r.PlatformPart.Minor);
        Assert.Equal(0, Postings.RefundAccrued(r).Lines.Sum(l => l.Amount.Minor));
    }

    [Theory]
    [InlineData("rub")]
    [InlineData("RU")]
    [InlineData("")]
    public void Плохая_валюта_отклоняется(string code) =>
        Assert.Throws<DomainException>(() => new Currency(code));
}
