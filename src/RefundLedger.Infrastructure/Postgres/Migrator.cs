using System.Reflection;
using Dapper;
using Npgsql;

namespace RefundLedger.Infrastructure.Postgres;

public sealed class Migrator(NpgsqlDataSource db)
{
    private const long LockKey = 7_203_001;

    public async Task RunAsync(CancellationToken ct = default)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Несколько подов стартуют одновременно: миграции накатывает первый.
        await conn.ExecuteAsync("select pg_advisory_xact_lock(@LockKey)", new { LockKey }, tx);
        await conn.ExecuteAsync(
            "create table if not exists schema_migrations (name text primary key, applied_at timestamptz not null default now())",
            transaction: tx);
        var applied = (await conn.QueryAsync<string>("select name from schema_migrations", transaction: tx)).ToHashSet();

        var assembly = Assembly.GetExecutingAssembly();
        foreach (var name in assembly.GetManifestResourceNames().Where(n => n.EndsWith(".sql")).Order())
        {
            if (applied.Contains(name)) continue;
            using var reader = new StreamReader(assembly.GetManifestResourceStream(name)!);
            await conn.ExecuteAsync(await reader.ReadToEndAsync(ct), transaction: tx);
            await conn.ExecuteAsync("insert into schema_migrations (name) values (@name)", new { name }, tx);
        }

        await tx.CommitAsync(ct);
    }
}
