using System.Diagnostics;
using Dapper;
using Microsoft.Extensions.Options;
using Npgsql;
using RefundLedger.Application;
using RefundLedger.Domain;
using RefundLedger.Infrastructure.Postgres;

// Запуск: dotnet run -c Release --project tools/RefundLedger.Bench -- "Host=...;Database=postgres" [секунд] [строк журнала]
var server = args.ElementAtOrDefault(0) ?? throw new ArgumentException("Нужна строка подключения");
var seconds = int.Parse(args.ElementAtOrDefault(1) ?? "10");
var journalRows = int.Parse(args.ElementAtOrDefault(2) ?? "1000000");
const int Sellers = 2_000;

Console.WriteLine("| Режим общего счёта | Потоков | Возвратов/с | p50, мс | p99, мс |");
Console.WriteLine("|---|---|---|---|---|");
// Режимы чередуются по раундам, чтобы прогрев и фон не достались одному из них.
foreach (var workers in new[] { 1, 8, 32 })
foreach (var mode in new[] { "strict", "delta", "strict", "delta" })
    await WithDatabaseAsync(async (db, cs) => await WritesAsync(db, cs, mode, workers));

await WithDatabaseAsync(async (db, _) => await ReadsAsync(db));

async Task WritesAsync(NpgsqlDataSource db, string cs, string mode, int workers)
{
    await using (var conn = await db.OpenConnectionAsync())
        await conn.ExecuteAsync(
            """
            insert into accounts (code, currency, mode) values ('in_transit:RUB', 'RUB', @mode);
            insert into accounts (code, currency, mode, floor_minor, balance_minor)
            select 'seller:' || g || ':RUB', 'RUB', 'strict', 0, 1000000000 from generate_series(1, @Sellers) g;
            """, new { mode, Sellers });

    await using var directory = new AccountDirectoryDb(cs);
    var register = new RegisterRefund(
        new LedgerSessionFactory(db, new AccountDirectory(directory, Options.Create(new LedgerOptions()))),
        new LedgerReader(db), TimeProvider.System);

    for (var i = 0; i < 300; i++) await OneAsync(register, i);

    var latencies = new List<double>[workers];
    var clock = Stopwatch.StartNew();
    await Task.WhenAll(Enumerable.Range(0, workers).Select(w => Task.Run(async () =>
    {
        var mine = latencies[w] = [];
        while (clock.Elapsed.TotalSeconds < seconds)
        {
            var start = Stopwatch.GetTimestamp();
            await OneAsync(register, Random.Shared.Next());
            mine.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }
    })));
    var total = clock.Elapsed.TotalSeconds;
    var all = latencies.SelectMany(x => x).Order().ToList();
    Console.WriteLine($"| {mode} | {workers} | {all.Count / total:F0} | {all[all.Count / 2]:F1} | {all[(int)(all.Count * 0.99)]:F1} |");

    var maintenance = new Maintenance(db);
    while (await maintenance.RollUpAsync(50_000, default) > 0) { }
    await using var check = await db.OpenConnectionAsync();
    var imbalance = await check.ExecuteScalarAsync<long>("select sum(amount_minor)::bigint from entries");
    if (imbalance != 0) throw new InvalidOperationException($"Леджер не сошёлся: {imbalance}");
}

static Task OneAsync(RegisterRefund register, int seed) => register.ExecuteAsync(
    new(Guid.NewGuid(), seed % 2_000 + 1, seed % Sellers + 1, Money.Positive(100, "RUB")), default);

async Task ReadsAsync(NpgsqlDataSource db)
{
    await using var conn = await db.OpenConnectionAsync();
    await conn.ExecuteAsync(new CommandDefinition(
        """
        insert into accounts (code, currency, mode) values ('in_transit:RUB', 'RUB', 'delta'), ('psp_clearing:RUB', 'RUB', 'delta');
        -- Массовая заливка: без триггеров и проверок, данные заведомо сбалансированы.
        set session_replication_role = replica;
        insert into postings (key, currency) select 'p' || g, 'RUB' from generate_series(1, @n) g;
        insert into entries (posting_id, account_id, currency, amount_minor)
        select p.id, a.id, 'RUB', case when a.code like 'in%' then -100 else 100 end from postings p, accounts a;
        set session_replication_role = origin;
        """, new { n = journalRows / 2 }, commandTimeout: 600));
    // Точка с запятой обязательна: одно слово Dapper считает именем процедуры.
    await conn.ExecuteAsync("analyze;");

    var depth = journalRows / 2 - 1_000;
    const string Head = "select e.id, p.key, e.amount_minor from entries e join postings p on p.id = e.posting_id where e.account_id = 1";
    var before = await conn.ExecuteScalarAsync<long>(
        "select id from entries where account_id = 1 order by id desc offset @depth limit 1", new { depth });
    Console.WriteLine($"\nСтраница из 100 строк на глубине {depth:N0}, журнал {journalRows:N0} строк:\n");
    Console.WriteLine("| Запрос | Медиана, мс |\n|---|---|");
    Console.WriteLine($"| OFFSET | {await MedianAsync($"{Head} order by e.id desc offset {depth} limit 100"):F2} |");
    Console.WriteLine($"| Курсор | {await MedianAsync($"{Head} and e.id < {before} order by e.id desc limit 100"):F2} |");

    async Task<double> MedianAsync(string sql)
    {
        var times = new List<double>();
        for (var i = 0; i < 15; i++)
        {
            var start = Stopwatch.GetTimestamp();
            (await conn.QueryAsync(sql)).Count();
            times.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }

        return times.Order().ElementAt(times.Count / 2);
    }
}

async Task WithDatabaseAsync(Func<NpgsqlDataSource, string, Task> body)
{
    var name = "bench_" + Guid.NewGuid().ToString("N");
    await using var admin = NpgsqlDataSource.Create(server);
    await using (var conn = await admin.OpenConnectionAsync()) await conn.ExecuteAsync($"create database {name}");
    var cs = new NpgsqlConnectionStringBuilder(server) { Database = name, MaxPoolSize = 40 }.ConnectionString;
    try
    {
        await using var db = NpgsqlDataSource.Create(cs);
        await new Migrator(db).RunAsync();
        await body(db, cs);
    }
    finally
    {
        await using var conn = await admin.OpenConnectionAsync();
        await conn.ExecuteAsync($"drop database {name} with (force)");
    }
}
