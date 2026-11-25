using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Dapper;
using Npgsql;

namespace RefundLedger.Infrastructure.Kafka;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string EventsTopic { get; set; } = "ledger.refund-events.v1";
    public string RefundsTopic { get; set; } = "returns.refund-approved.v1";
    public string DeadLetterTopic { get; set; } = "returns.refund-approved.v1.dlq";
    public string GroupId { get; set; } = "refund-ledger";
}

/// <summary>
/// Сначала публикация, потом удаление: падение между ними даёт дубль, но не потерю.
/// </summary>
public sealed class OutboxPublisher(NpgsqlDataSource db, IProducer<string, string> producer, KafkaOptions options)
{
    public static readonly ActivitySource Source = new("RefundLedger.Kafka");
    private const long LockKey = 7_203_003;

    public async Task<int> PublishAsync(int limit, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Публикует только один экземпляр: иначе два события одного возврата
        // разъехались бы по разным процессам и поменялись местами.
        if (!await conn.ExecuteScalarAsync<bool>(new CommandDefinition(
                "select pg_try_advisory_xact_lock(@LockKey)", new { LockKey }, tx, cancellationToken: ct)))
            return 0;

        var rows = (await conn.QueryAsync<(long Id, string Key, string Type, string Payload, string? Trace)>(
            new CommandDefinition(
                "select id, message_key, type, payload::text, trace_parent from outbox order by id limit @limit",
                new { limit }, tx, cancellationToken: ct))).ToList();
        if (rows.Count == 0) return 0;

        await Task.WhenAll(rows.Select(r =>
        {
            ActivityContext.TryParse(r.Trace, null, out var parent);
            using var activity = Source.StartActivity("outbox publish", ActivityKind.Producer, parent);
            var headers = new Headers { { "type", Encoding.UTF8.GetBytes(r.Type) } };
            if ((activity?.Id ?? r.Trace) is { } trace)
                headers.Add("traceparent", Encoding.UTF8.GetBytes(trace));
            return producer.ProduceAsync(options.EventsTopic,
                new Message<string, string> { Key = r.Key, Value = r.Payload, Headers = headers }, ct);
        }));

        await conn.ExecuteAsync(new CommandDefinition("delete from outbox where id = any(@ids)",
            new { ids = rows.Select(r => r.Id).ToArray() }, tx, cancellationToken: ct));
        await tx.CommitAsync(ct);
        return rows.Count;
    }

    public static ProducerConfig ProducerConfig(string bootstrap) => new()
    {
        BootstrapServers = bootstrap,
        EnableIdempotence = true,
        Acks = Acks.All,
        LingerMs = 5,
        MessageTimeoutMs = 10_000
    };
}
