using Dapper;
using Npgsql;
using RefundLedger.Application;
using RefundLedger.Domain;

namespace RefundLedger.IntegrationTests;

public class LedgerTests(PostgresServer s) : LedgerTest(s)
{
    [Fact]
    public async Task Сто_одинаковых_запросов_создают_одну_выплату()
    {
        await CreditAsync(1, 10_000);
        var id = Guid.NewGuid();

        var results = await Task.WhenAll(Enumerable.Range(0, 100).Select(_ => RefundAsync(7, 1, 1_000, id)));

        Assert.All(results, r => Assert.Equal(1_000, r.SellerPart.Minor));
        Assert.Equal(1, await ScalarAsync<long>("select count(*) from refunds"));
        Assert.Equal(1, await ScalarAsync<long>("select count(*) from payouts"));
        Assert.Equal(9_000, await BalanceAsync(AccountCode.Seller(1, Rub)));
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task Тот_же_id_с_другой_суммой_отклоняется()
    {
        var id = Guid.NewGuid();
        await RefundAsync(7, 1, 1_000, id);
        await Assert.ThrowsAsync<ConflictException>(() => RefundAsync(7, 1, 2_000, id));
    }

    [Fact]
    public async Task Параллельные_возвраты_не_делят_один_остаток_дважды()
    {
        await CreditAsync(1, 10_550);

        var refunds = await Task.WhenAll(Enumerable.Range(1, 200).Select(b => RefundAsync(b, 1, 100)));

        Assert.Equal(10_550, refunds.Sum(r => r.SellerPart.Minor));
        Assert.Equal(0, await BalanceAsync(AccountCode.Seller(1, Rub)));
        Assert.Equal(-9_450, await BalanceAsync(AccountCode.Compensation(Rub)));
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task Начисление_продавцу_идемпотентно()
    {
        var results = await Task.WhenAll(Enumerable.Range(0, 30)
            .Select(_ => Credit.ExecuteAsync("op-1", 5, Money.Positive(500, "RUB"), default)));

        Assert.Single(results, duplicate => !duplicate);
        Assert.Equal(500, await BalanceAsync(AccountCode.Seller(5, Rub)));
    }

    [Fact]
    public async Task Смешанная_нагрузка_со_свёрткой_не_теряет_денег_и_не_виснет()
    {
        using var stop = new CancellationTokenSource();
        var rollups = Enumerable.Range(0, 2).Select(_ => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested) await Maintenance.RollUpAsync(50, default);
        })).ToArray();

        // Владельцы пересекаются намеренно: так проявились бы взаимные блокировки.
        await Parallel.ForEachAsync(Enumerable.Range(0, 600), new ParallelOptions { MaxDegreeOfParallelism = 16 },
            async (i, _) =>
            {
                if (i % 3 == 0) await CreditAsync(i % 4 + 1, 300);
                else await RefundAsync(i % 5 + 1, i % 4 + 1, 100 + i % 50);
            });
        await stop.CancelAsync();
        await Task.WhenAll(rollups);

        while (await Maintenance.RollUpAsync(1_000, default) > 0) { }
        Assert.Equal(0, await ScalarAsync<long>("select count(*) from balance_deltas"));
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task Параллельные_свёртки_не_блокируют_друг_друга_намертво()
    {
        const int accounts = 40;
        await using (var conn = await Db.OpenConnectionAsync())
            await conn.ExecuteAsync(
                "insert into accounts (code, currency, mode) select 'd' || g, 'RUB', 'delta' from generate_series(1, @accounts) g",
                new { accounts });

        for (var round = 0; round < 30; round++)
        {
            await using (var conn = await Db.OpenConnectionAsync())
                await conn.ExecuteAsync(
                    "insert into balance_deltas (account_id, amount_minor) select id, 1 from accounts, generate_series(1, 20) order by random()");
            await Task.WhenAll(Enumerable.Range(0, 6).Select(async _ =>
            {
                while (await Maintenance.RollUpAsync(60, default) > 0) { }
            }));
        }

        while (await Maintenance.RollUpAsync(1_000, default) > 0) { }
        Assert.Equal(accounts * 20 * 30, await ScalarAsync<long>("select sum(balance_minor)::bigint from accounts"));
    }

    [Fact]
    public async Task Малый_пул_не_приводит_к_взаимоблокировке_соединений()
    {
        // Все счета новые: каждый запрос ищет счёт, уже держа соединение.
        await Parallel.ForEachAsync(Enumerable.Range(1, 200), new ParallelOptions { MaxDegreeOfParallelism = 60 },
            async (i, _) => await RefundAsync(1000 + i, 5000 + i, 10));
        await AssertLedgerConsistentAsync();
    }

    [Fact]
    public async Task БД_не_примет_несбалансированную_проводку()
    {
        await CreditAsync(1, 100);
        await using var conn = await Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await conn.ExecuteAsync(
            """
            with p as (insert into postings (key, currency) values ('bad', 'RUB') returning id)
            insert into entries (posting_id, account_id, currency, amount_minor)
            select p.id, a.id, 'RUB', 10 from p, accounts a
            """, transaction: tx);

        await Assert.ThrowsAsync<PostgresException>(() => tx.CommitAsync());
    }

    [Fact]
    public async Task Дописать_строку_в_чужую_проводку_нельзя()
    {
        await CreditAsync(1, 100);
        await using var conn = await Db.OpenConnectionAsync();
        await Assert.ThrowsAsync<PostgresException>(() => conn.ExecuteAsync(
            "insert into entries (posting_id, account_id, currency, amount_minor) select min(posting_id), min(account_id), 'RUB', 5 from entries"));
    }

    [Theory]
    [InlineData("update entries set amount_minor = 1")]
    [InlineData("delete from entries")]
    [InlineData("update postings set key = 'x'")]
    [InlineData("truncate entries")]
    [InlineData("truncate entries_default")]
    [InlineData("truncate postings cascade")]
    [InlineData("update accounts set balance_minor = -1 where mode = 'strict'")]
    public async Task БД_защищает_журнал_и_остатки(string sql)
    {
        await CreditAsync(1, 100);
        await using var conn = await Db.OpenConnectionAsync();
        await Assert.ThrowsAsync<PostgresException>(() => conn.ExecuteAsync(sql));
    }

    [Fact]
    public async Task Журнал_листается_без_пропусков_и_повторов()
    {
        for (var i = 1; i <= 25; i++) await CreditAsync(1, i);
        var ids = new List<long>();
        long? cursor = null;
        while (true)
        {
            var page = await Reader.ListEntriesAsync("seller:1:RUB", cursor, 10, default);
            if (page.Count == 0) break;
            ids.AddRange(page.Select(e => e.Id));
            cursor = page[^1].Id;
        }

        Assert.Equal(25, ids.Count);
        Assert.Equal(ids.Distinct().OrderDescending(), ids);
    }
}
