using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RefundLedger.Infrastructure.Kafka;

namespace RefundLedger.IntegrationTests;

public class KafkaTests(PostgresServer s) : LedgerTest(s)
{
    /// <summary>Mock-кластер librdkafka: настоящий протокол на локальном порту, без Docker.</summary>
    private static (IProducer<string, string> Owner, KafkaOptions Options) StartKafka()
    {
        var owner = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = "unused:9092",
            AllowAutoCreateTopics = true
        }.Concat([new("test.mock.num.brokers", "1")])).Build();
        using var admin = new DependentAdminClientBuilder(owner.Handle).Build();
        var broker = admin.GetMetadata(TimeSpan.FromSeconds(10)).Brokers[0];
        return (owner, new KafkaOptions { BootstrapServers = $"{broker.Host}:{broker.Port}" });
    }

    private static IProducer<string, string> Producer(KafkaOptions o) =>
        new ProducerBuilder<string, string>(OutboxPublisher.ProducerConfig(o.BootstrapServers)).Build();

    private static List<ConsumeResult<string, string>> ReadAll(KafkaOptions o, string topic, int expected)
    {
        using var c = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = o.BootstrapServers, GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        c.Subscribe(topic);
        var result = new List<ConsumeResult<string, string>>();
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (result.Count < expected && DateTime.UtcNow < deadline)
        {
            try
            {
                if (c.Consume(TimeSpan.FromMilliseconds(200)) is { } m) result.Add(m);
            }
            catch (ConsumeException e) when (e.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                // Топик появится с первым сообщением.
                Thread.Sleep(200);
            }
        }
        c.Close();
        return result;
    }

    [Fact]
    public async Task Событие_доходит_с_ключом_возврата_и_уходит_из_outbox()
    {
        var (owner, options) = StartKafka();
        using var _ = owner;
        using var producer = Producer(options);
        var r = await RefundAsync(7, 1, 1_000);

        Assert.Equal(1, await new OutboxPublisher(Db, producer, options).PublishAsync(100, default));

        var m = Assert.Single(ReadAll(options, options.EventsTopic, 1));
        Assert.Equal(r.Id.ToString(), m.Message.Key);
        Assert.Equal("refund.registered", JsonDocument.Parse(m.Message.Value).RootElement.GetProperty("type").GetString());
        Assert.Equal(0, await ScalarAsync<long>("select count(*) from outbox"));
    }

    [Fact]
    public async Task Kafka_недоступна_событие_остаётся_и_уходит_позже()
    {
        await RefundAsync(7, 1, 1_000);
        var config = OutboxPublisher.ProducerConfig("127.0.0.1:1");
        config.MessageTimeoutMs = 1_000;
        using (var dead = new ProducerBuilder<string, string>(config).Build())
            await Assert.ThrowsAnyAsync<KafkaException>(() =>
                new OutboxPublisher(Db, dead, new KafkaOptions()).PublishAsync(100, default));
        Assert.Equal(1, await ScalarAsync<long>("select count(*) from outbox"));

        var (owner, options) = StartKafka();
        using var _ = owner;
        using var producer = Producer(options);
        Assert.Equal(1, await new OutboxPublisher(Db, producer, options).PublishAsync(100, default));
    }

    [Fact]
    public async Task Публикует_только_один_экземпляр()
    {
        await RefundAsync(7, 1, 1_000);
        await using var conn = await Db.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using (var cmd = new Npgsql.NpgsqlCommand("select pg_advisory_xact_lock(7203003)", conn, tx))
            await cmd.ExecuteNonQueryAsync();

        var (owner, options) = StartKafka();
        using var _ = owner;
        using var producer = Producer(options);
        Assert.Equal(0, await new OutboxPublisher(Db, producer, options).PublishAsync(100, default));
    }

    [Fact]
    public async Task Дубль_безвреден_а_битое_сообщение_не_останавливает_партицию()
    {
        var (owner, options) = StartKafka();
        using var _ = owner;
        using var producer = Producer(options);
        var good = JsonSerializer.Serialize(new RefundApproved(Guid.NewGuid(), 1, 2, 500, "RUB"), JsonSerializerOptions.Web);
        var last = JsonSerializer.Serialize(new RefundApproved(Guid.NewGuid(), 3, 2, 700, "RUB"), JsonSerializerOptions.Web);
        foreach (var body in new[] { good, "{битый json", good, """{"amountMinor":-5,"currency":"RUB"}""", last })
            await producer.ProduceAsync(options.RefundsTopic, new() { Key = "k", Value = body });

        var services = new ServiceCollection().AddTransient(_ => Register).BuildServiceProvider();
        var consumer = new RefundConsumer(services.GetRequiredService<IServiceScopeFactory>(), producer, options,
            NullLogger<RefundConsumer>.Instance);
        await consumer.StartAsync(default);
        var dead = ReadAll(options, options.DeadLetterTopic, 2);
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (await ScalarAsync<long>("select count(*) from refunds") < 2 && DateTime.UtcNow < deadline)
            await Task.Delay(100);
        await consumer.StopAsync(default);

        Assert.Equal(2, dead.Count);
        Assert.Equal(2, await ScalarAsync<long>("select count(*) from refunds"));
        await AssertLedgerConsistentAsync();
    }
}
