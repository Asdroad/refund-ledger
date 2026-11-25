using Dapper;
using RefundLedger.Application;
using RefundLedger.Domain;
using RefundLedger.Infrastructure.Psp;

namespace RefundLedger.IntegrationTests;

public class PayoutFlowTests(PostgresServer s) : LedgerTest(s)
{
    private Task<int> SendAsync() => Process.ExecuteAsync(PayoutQueue.ToSend, default);
    private Task<int> ResolveAsync() => Process.ExecuteAsync(PayoutQueue.ToResolve, default);

    private async Task AssertPaidOnceAsync(Refund r)
    {
        Assert.Equal(RefundStatus.Paid, (await Reader.FindRefundAsync(r.Id, default))!.Status);
        Assert.Equal(1, await PspCountAsync());
        Assert.Equal(0, await BalanceAsync(AccountCode.InTransit(Rub)));
        Assert.Equal(1_000, await BalanceAsync(AccountCode.PspClearing(Rub)));
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task Успешная_выплата()
    {
        await CreditAsync(1, 5_000);
        var r = await RefundAsync(7, 1, 1_000);
        await SendAsync();
        await AssertPaidOnceAsync(r);
    }

    [Theory]
    [InlineData("error_after")]
    [InlineData("hang_after")]
    public async Task Деньги_ушли_а_ответ_потерян_второго_перевода_нет(string mode)
    {
        await ModeAsync(mode);
        var r = await RefundAsync(7, 1, 1_000);
        await SendAsync();
        Assert.Equal((short)PayoutStatus.Unknown, await ScalarAsync<short>("select status from payouts"));
        Assert.Equal(1_000, await BalanceAsync(AccountCode.InTransit(Rub)));

        Clock.Offset = TimeSpan.FromMinutes(10);
        await ResolveAsync();
        await AssertPaidOnceAsync(r);
    }

    [Fact]
    public async Task Запрос_не_дошёл_повтор_только_после_паузы()
    {
        await ModeAsync("error_before");
        var r = await RefundAsync(7, 1, 1_000);
        await SendAsync();

        Clock.Offset = TimeSpan.FromSeconds(10);
        await ResolveAsync();
        Assert.Equal((short)PayoutStatus.Unknown, await ScalarAsync<short>("select status from payouts"));

        await ModeAsync("ok");
        Clock.Offset = TimeSpan.FromMinutes(10);
        await ResolveAsync();
        Assert.Equal((short)PayoutStatus.Pending, await ScalarAsync<short>("select status from payouts"));
        await SendAsync();
        await AssertPaidOnceAsync(r);
    }

    [Fact]
    public async Task Троттлинг_откладывает_отправку()
    {
        await ModeAsync("throttle");
        var r = await RefundAsync(7, 1, 1_000);
        await SendAsync();
        Assert.Equal(0, await SendAsync());

        await ModeAsync("ok");
        Clock.Offset = TimeSpan.FromMinutes(10);
        await SendAsync();
        await AssertPaidOnceAsync(r);
    }

    [Fact]
    public async Task Отказ_возвращает_деньги_на_счёт_а_повтор_идёт_с_новым_ключом()
    {
        await ModeAsync("decline");
        var r = await RefundAsync(7, 1, 1_000);
        await SendAsync();
        Assert.Equal(RefundStatus.PayoutRejected, (await Reader.FindRefundAsync(r.Id, default))!.Status);
        Assert.Equal(1_000, await BalanceAsync(AccountCode.Buyer(7, Rub)));

        await ModeAsync("ok");
        await new RetryPayout(Sessions, Clock).ExecuteAsync(r.Id, default);
        await SendAsync();

        Assert.Equal(RefundStatus.Paid, (await Reader.FindRefundAsync(r.Id, default))!.Status);
        Assert.Equal(0, await BalanceAsync(AccountCode.Buyer(7, Rub)));
        Assert.Equal(2, await ScalarAsync<long>("select count(distinct id) from payouts"));
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task Параллельный_ручной_повтор_создаёт_одну_выплату()
    {
        await ModeAsync("decline");
        var r = await RefundAsync(7, 1, 1_000);
        await SendAsync();

        var attempts = await Task.WhenAll(Enumerable.Range(0, 20).Select(async _ =>
        {
            try
            {
                await new RetryPayout(Sessions, Clock).ExecuteAsync(r.Id, default);
                return true;
            }
            catch (InvalidTransitionException)
            {
                return false;
            }
        }));

        Assert.Single(attempts, ok => ok);
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task Воркер_умер_после_отправки_итог_узнаём_у_провайдера()
    {
        var r = await RefundAsync(7, 1, 1_000);
        // Деньги отправлены, но процесс «убит» до записи результата.
        await using (var tx = await Sessions.BeginAsync(default))
        {
            var p = Assert.Single(await tx.TakeDueAsync(PayoutQueue.ToSend, Clock.GetUtcNow(), 10, default));
            p.Lease(Clock.GetUtcNow(), Payouts.Lease);
            await tx.SavePayoutAsync(p, default);
            await tx.CommitAsync(default);
            await Psp.SendAsync(p, default);
        }

        Assert.Equal(0, await ResolveAsync());
        Clock.Offset = Payouts.Lease + TimeSpan.FromSeconds(1);
        Assert.Equal(1, await ResolveAsync());
        await AssertPaidOnceAsync(r);
    }

    [Fact]
    public async Task Опоздавший_воркер_не_перезапишет_чужой_итог()
    {
        var r = await RefundAsync(7, 1, 1_000);
        Payout stale;
        await using (var tx = await Sessions.BeginAsync(default))
        {
            stale = Assert.Single(await tx.TakeDueAsync(PayoutQueue.ToSend, Clock.GetUtcNow(), 10, default));
            stale.Lease(Clock.GetUtcNow(), Payouts.Lease);
            await tx.SavePayoutAsync(stale, default);
            await tx.CommitAsync(default);
            await Psp.SendAsync(stale, default);
        }

        Clock.Offset = Payouts.Lease + TimeSpan.FromSeconds(1);
        await ResolveAsync();
        await Process.ApplyAsync(stale, new PspResult.Declined("late"), default);
        await AssertPaidOnceAsync(r);
    }

    [Fact]
    public async Task Несколько_воркеров_и_хаос_провайдера_все_деньги_на_месте()
    {
        await CreditAsync(1, 1_000_000);
        for (var i = 1; i <= 150; i++) await RefundAsync(i, 1, 100);
        await ModeAsync("chaos");
        for (var round = 0; round < 4; round++)
        {
            Clock.Offset += TimeSpan.FromMinutes(10);
            await Task.WhenAll(SendAsync(), SendAsync(), SendAsync(), ResolveAsync(), ResolveAsync());
        }

        await ModeAsync("ok");
        for (var round = 0; round < 40; round++)
        {
            Clock.Offset += TimeSpan.FromMinutes(10);
            await Task.WhenAll(SendAsync(), SendAsync(), ResolveAsync(), ResolveAsync());
        }

        Assert.Equal(0, await ScalarAsync<long>("select count(*) from payouts where status in (1, 2, 3)"));
        Assert.Equal(0, await BalanceAsync(AccountCode.InTransit(Rub)));
        var paid = await ScalarAsync<long>("select count(*) from payouts where status = 4");
        Assert.Equal(paid * 100, await BalanceAsync(AccountCode.PspClearing(Rub)));
        var registry = await Psp.GetRegistryAsync(0, 10_000, default);
        Assert.Equal(paid, registry.Count(o => o.Status == "succeeded"));
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task Сверка_находит_чужой_перевод_расхождение_суммы_и_статуса()
    {
        var ok = await RefundAsync(1, 1, 100);
        var wrongAmount = await RefundAsync(2, 1, 200);
        var wrongStatus = await RefundAsync(3, 1, 300);
        await SendAsync();
        await StubHttp.SendAsync(new HttpRequestMessage(HttpMethod.Post, "/v1/transfers")
        {
            Headers = { { "Idempotency-Key", Guid.NewGuid().ToString() } },
            Content = System.Net.Http.Json.JsonContent.Create(new { amountMinor = 999, currency = "RUB" })
        });
        await using (var conn = await Db.OpenConnectionAsync())
        {
            await conn.ExecuteAsync("update payouts set amount_minor = 201 where refund_id = @Id", new { wrongAmount.Id });
            await conn.ExecuteAsync("update payouts set status = 5 where refund_id = @Id", new { wrongStatus.Id });
        }

        var reconciler = new RegistryReconciler(Db, Psp);
        Assert.Equal(4, await reconciler.RunAsync(100, default));
        Assert.Equal(0, await reconciler.RunAsync(100, default));

        await using var c = await Db.OpenConnectionAsync();
        var kinds = await c.QueryAsync<string>("select kind from reconciliation_issues order by kind");
        Assert.Equal(["amount_mismatch", "missing_in_ledger", "status_conflict"], kinds);
        Assert.Equal(1, await ScalarAsync<long>(
            $"select count(*) from payouts where refund_id = '{ok.Id}' and registry_seen_at is not null"));
    }
}
