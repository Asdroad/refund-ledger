using RefundLedger.Domain;

namespace RefundLedger.UnitTests;

public class PayoutTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    private static Payout New()
    {
        var refund = Refund.Register(Guid.NewGuid(), 1, 2, Money.Positive(100, "RUB"), 100);
        return Payout.Create(Guid.NewGuid(), refund, Now, null);
    }

    [Fact]
    public void Пока_аренда_жива_выплату_не_перехватить()
    {
        var p = New();
        p.Lease(Now, Lease);
        Assert.Throws<InvalidTransitionException>(() => p.Lease(Now.AddSeconds(29), Lease));
    }

    [Fact]
    public void Истёкшая_аренда_означает_неизвестный_итог_а_не_повторную_отправку()
    {
        var p = New();
        p.Lease(Now, Lease);
        p.Lease(Now.AddSeconds(31), Lease);
        Assert.Equal(PayoutStatus.Unknown, p.Status);
        Assert.Equal(1, p.Attempts);
    }

    [Fact]
    public void Старый_владелец_аренды_теряет_право_записи()
    {
        var p = New();
        p.Lease(Now, Lease);
        var old = p.LeaseToken!.Value;
        p.Lease(Now.AddSeconds(31), Lease);
        Assert.False(p.HoldsLease(old));
    }

    [Fact]
    public void Таймаут_не_отказ_деньги_не_возвращаются()
    {
        var p = New();
        p.Lease(Now, Lease);
        p.MarkUnknown(Now.AddSeconds(5));
        Assert.Equal(PayoutStatus.Unknown, p.Status);
        Assert.False(p.IsTerminal);
    }

    [Fact]
    public void Завершённую_выплату_не_изменить()
    {
        var p = New();
        p.Lease(Now, Lease);
        p.Succeed("ref");
        Assert.Throws<InvalidTransitionException>(() => p.Reject("x"));
        Assert.Throws<InvalidTransitionException>(() => p.Resend(Now));
        Assert.Throws<InvalidTransitionException>(() => p.Lease(Now.AddDays(1), Lease));
    }

    [Fact]
    public void Нельзя_завершить_выплату_которую_не_отправляли() =>
        Assert.Throws<InvalidTransitionException>(() => New().Succeed("ref"));

    [Fact]
    public void Оплаченный_возврат_нельзя_повторить()
    {
        var r = Refund.Register(Guid.NewGuid(), 1, 2, Money.Positive(100, "RUB"), 100);
        r.MarkPaid();
        Assert.Throws<InvalidTransitionException>(r.Retry);
    }

    [Fact]
    public void Пауза_растёт_но_ограничена()
    {
        var random = new Random(1);
        Assert.InRange(Backoff.For(1, random).TotalSeconds, 1, 2);
        Assert.InRange(Backoff.For(100, random).TotalSeconds, 150, 300);
    }
}
