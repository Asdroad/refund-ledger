using System.Collections.Concurrent;
using Dapper;
using Microsoft.Extensions.Options;
using Npgsql;
using RefundLedger.Domain;

namespace RefundLedger.Infrastructure.Postgres;

public sealed record AccountRef(long Id, bool Strict);

/// <summary>
/// Отдельный маленький пул. Счёт ищут, уже держа соединение из основного пула: с общим пулом
/// под нагрузкой все ждали бы второго соединения друг от друга.
/// </summary>
public sealed class AccountDirectoryDb(string connectionString) : IAsyncDisposable
{
    public NpgsqlDataSource Source { get; } = NpgsqlDataSource.Create(
        new NpgsqlConnectionStringBuilder(connectionString) { MaxPoolSize = 4 });

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}

/// <summary>
/// Код счёта → id. Создаёт счёт отдельной транзакцией: пустой счёт безвреден, а вставка
/// внутри основной держала бы блокировку уникального индекса до её конца.
/// </summary>
public sealed class AccountDirectory(AccountDirectoryDb directory, IOptions<LedgerOptions> options)
{
    private const int MaxCached = 200_000;
    private readonly ConcurrentDictionary<AccountCode, AccountRef> _cache = new();

    public async Task<AccountRef> ResolveAsync(AccountCode code, CancellationToken ct)
    {
        if (_cache.TryGetValue(code, out var known)) return known;

        await using var conn = await directory.Source.OpenConnectionAsync(ct);
        long? floor = code.Kind switch
        {
            AccountKind.Seller => -options.Value.SellerCreditLimitMinor,
            AccountKind.Buyer => 0,
            _ => null
        };
        await conn.ExecuteAsync(new CommandDefinition(
            """
            insert into accounts (code, currency, mode, floor_minor) values (@Code, @Currency, @Mode, @floor)
            on conflict (code) do nothing
            """,
            new
            {
                Code = code.ToString(), Currency = code.Currency.Code,
                Mode = code.Mode == BalanceMode.Strict ? "strict" : "delta", floor
            }, cancellationToken: ct));
        var (id, mode) = await conn.QuerySingleAsync<(long, string)>(new CommandDefinition(
            "select id, mode from accounts where code = @Code", new { Code = code.ToString() }, cancellationToken: ct));

        var found = new AccountRef(id, mode == "strict");
        // id и режим не меняются, поэтому кэш сбрасывают только ради памяти.
        if (_cache.Count >= MaxCached) _cache.Clear();
        _cache[code] = found;
        return found;
    }
}
