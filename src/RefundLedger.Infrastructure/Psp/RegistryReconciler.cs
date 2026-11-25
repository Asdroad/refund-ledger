using Dapper;
using Npgsql;
using RefundLedger.Application;
using RefundLedger.Domain;

namespace RefundLedger.Infrastructure.Psp;

/// <summary>Сверка с реестром провайдера: ловит то, что не видно по одной выплате.</summary>
public sealed class RegistryReconciler(NpgsqlDataSource db, IPspClient psp)
{
    private sealed record PayoutInfo(Guid Id, long AmountMinor, string Currency, short Status);

    public async Task<int> RunAsync(int limit, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var cursor = await conn.ExecuteScalarAsync<long>(new CommandDefinition(
            "select last_seq from registry_cursor where id = 1", cancellationToken: ct));
        var operations = await psp.GetRegistryAsync(cursor, limit, ct);
        if (operations.Count == 0) return 0;

        await using var tx = await conn.BeginTransactionAsync(ct);
        var locked = await conn.ExecuteScalarAsync<long>(new CommandDefinition(
            "select last_seq from registry_cursor where id = 1 for update", transaction: tx, cancellationToken: ct));
        if (locked != cursor) return 0;

        var keys = operations.Select(o => o.Key).ToArray();
        var payouts = (await conn.QueryAsync<PayoutInfo>(new CommandDefinition(
            """
            select id as Id, amount_minor as AmountMinor, currency as Currency, status as Status
            from payouts where id = any(@keys)
            """, new { keys }, tx, cancellationToken: ct))).ToDictionary(p => p.Id);

        foreach (var op in operations)
        {
            var issue = Compare(op, payouts.GetValueOrDefault(op.Key));
            if (issue is not var (kind, details)) continue;
            await conn.ExecuteAsync(new CommandDefinition(
                """
                insert into reconciliation_issues (psp_seq, payout_key, kind, details)
                values (@Seq, @Key, @kind, @details) on conflict do nothing
                """, new { op.Seq, op.Key, kind, details }, tx, cancellationToken: ct));
        }

        await conn.ExecuteAsync(new CommandDefinition(
            "update payouts set registry_seen_at = now() where id = any(@keys)", new { keys }, tx, cancellationToken: ct));
        await conn.ExecuteAsync(new CommandDefinition(
            "update registry_cursor set last_seq = @last where id = 1",
            new { last = operations[^1].Seq }, tx, cancellationToken: ct));
        await tx.CommitAsync(ct);
        return operations.Count;
    }

    private static (string Kind, string Details)? Compare(PspOperation op, PayoutInfo? p)
    {
        if (p is null) return ("missing_in_ledger", $"{op.AmountMinor} {op.Currency} {op.Status}");
        if (p.AmountMinor != op.AmountMinor || p.Currency != op.Currency)
            return ("amount_mismatch", $"леджер {p.AmountMinor} {p.Currency}, провайдер {op.AmountMinor} {op.Currency}");

        // Незавершённая у нас выплата — не расхождение: её закроет опрос статуса.
        var conflict = (PayoutStatus)p.Status switch
        {
            PayoutStatus.Succeeded => op.Status != "succeeded",
            PayoutStatus.Rejected => op.Status == "succeeded",
            _ => false
        };
        return conflict ? ("status_conflict", $"леджер {(PayoutStatus)p.Status}, провайдер {op.Status}") : null;
    }
}
