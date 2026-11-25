extern alias psp;
using Dapper;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Options;
using Npgsql;
using RefundLedger.Application;
using RefundLedger.Domain;
using RefundLedger.Infrastructure.Postgres;
using RefundLedger.Infrastructure.Psp;
using Testcontainers.PostgreSql;

namespace RefundLedger.IntegrationTests;

/// <summary>Сервер БД один на прогон. LEDGER_TEST_PG позволяет обойтись без Docker.</summary>
public sealed class PostgresServer : IAsyncLifetime
{
    private PostgreSqlContainer? _container;
    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("LEDGER_TEST_PG") is { Length: > 0 } external)
        {
            ConnectionString = external;
            return;
        }

        _container = new PostgreSqlBuilder("postgres:17-alpine").Build();
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public Task DisposeAsync() => _container?.DisposeAsync().AsTask() ?? Task.CompletedTask;
}

[CollectionDefinition("pg")]
public sealed class PostgresCollection : ICollectionFixture<PostgresServer>;

public sealed class TestClock : TimeProvider
{
    public TimeSpan Offset { get; set; }
    public override DateTimeOffset GetUtcNow() => DateTimeOffset.UtcNow + Offset;
}

/// <summary>Чистая база и собранные сценарии на каждый тест.</summary>
[Collection("pg")]
public abstract class LedgerTest(PostgresServer server) : IAsyncLifetime
{
    protected static readonly Currency Rub = new("RUB");
    private readonly string _dbName = "t_" + Guid.NewGuid().ToString("N");

    protected string ConnectionString { get; private set; } = "";
    protected NpgsqlDataSource Db { get; private set; } = null!;
    protected AccountDirectoryDb DirectoryDb { get; private set; } = null!;
    protected ILedgerSessionFactory Sessions { get; private set; } = null!;
    protected LedgerReader Reader { get; private set; } = null!;
    protected Maintenance Maintenance { get; private set; } = null!;
    protected TestClock Clock { get; } = new();
    protected PayoutOptions Payouts { get; } = new();

    protected WebApplicationFactory<psp::Program> Stub { get; private set; } = null!;
    protected HttpClient StubHttp { get; private set; } = null!;
    protected IPspClient Psp { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await using (var admin = NpgsqlDataSource.Create(server.ConnectionString))
        await using (var cmd = admin.CreateCommand($"create database {_dbName}"))
            await cmd.ExecuteNonQueryAsync();

        ConnectionString = new NpgsqlConnectionStringBuilder(server.ConnectionString)
            { Database = _dbName, MaxPoolSize = 20 }.ConnectionString;
        Db = NpgsqlDataSource.Create(ConnectionString);
        await new Migrator(Db).RunAsync();
        DirectoryDb = new AccountDirectoryDb(ConnectionString);
        Sessions = new LedgerSessionFactory(Db, new AccountDirectory(DirectoryDb, Options.Create(new LedgerOptions())));
        Reader = new LedgerReader(Db);
        Maintenance = new Maintenance(Db);

        Stub = new WebApplicationFactory<psp::Program>();
        StubHttp = Stub.CreateClient();
        StubHttp.Timeout = TimeSpan.FromSeconds(2);
        Psp = new HttpPspClient(StubHttp);
    }

    public async Task DisposeAsync()
    {
        await Stub.DisposeAsync();
        await DirectoryDb.DisposeAsync();
        await Db.DisposeAsync();
        NpgsqlConnection.ClearAllPools();
        await using var admin = NpgsqlDataSource.Create(server.ConnectionString);
        await using var cmd = admin.CreateCommand($"drop database {_dbName} with (force)");
        await cmd.ExecuteNonQueryAsync();
    }

    protected RegisterRefund Register => new(Sessions, Reader, Clock);
    protected CreditSeller Credit => new(Sessions);
    protected ProcessPayouts Process => new(Sessions, Psp, Clock, Payouts);

    protected Task<Refund> RefundAsync(long buyer, long seller, long minor, Guid? id = null) =>
        Register.ExecuteAsync(new(id ?? Guid.NewGuid(), buyer, seller, Money.Positive(minor, "RUB")), default);

    protected Task CreditAsync(long seller, long minor) =>
        Credit.ExecuteAsync(Guid.NewGuid().ToString(), seller, Money.Positive(minor, "RUB"), default);

    protected Task ModeAsync(string mode) => StubHttp.PostAsync($"/admin/mode/{mode}", null);

    protected async Task<int> PspCountAsync() => (await Psp.GetRegistryAsync(0, 10_000, default)).Count;

    protected Task<long?> BalanceAsync(AccountCode code) => Reader.GetBalanceAsync(code.ToString(), default);

    protected async Task<T> ScalarAsync<T>(string sql)
    {
        await using var conn = await Db.OpenConnectionAsync();
        return (await conn.ExecuteScalarAsync<T>(sql))!;
    }

    protected async Task AssertLedgerConsistentAsync() =>
        Assert.Equal((0L, 0L), await Maintenance.SelfCheckAsync(default));
}
