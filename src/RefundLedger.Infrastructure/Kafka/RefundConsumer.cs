using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RefundLedger.Application;
using RefundLedger.Domain;

namespace RefundLedger.Infrastructure.Kafka;

public sealed record RefundApproved(Guid RefundId, long BuyerId, long SellerId, long AmountMinor, string Currency);

/// <summary>
/// Отдельный inbox не нужен: сценарий идемпотентен по id возврата, повтор сообщения безвреден.
/// </summary>
public sealed class RefundConsumer(
    IServiceScopeFactory scopes, IProducer<string, string> producer, KafkaOptions options,
    ILogger<RefundConsumer> log) : BackgroundService
{
    // Consume блокирует поток, поэтому у потребителя он свой.
    protected override Task ExecuteAsync(CancellationToken ct) =>
        Task.Factory.StartNew(() => LoopAsync(ct), CancellationToken.None,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task LoopAsync(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // Смещение сохраняем сами, после записи в БД.
            EnableAutoOffsetStore = false
        }).Build();
        consumer.Subscribe(options.RefundsTopic);

        var failures = 0;
        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<string, string>? message = null;
            try
            {
                message = consumer.Consume(ct);
                await HandleAsync(message, ct);
                consumer.StoreOffset(message);
                failures = 0;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                log.LogError(e, "Сообщение не обработано, повторим");
                await Task.Delay(Backoff.For(++failures, Random.Shared, TimeSpan.FromSeconds(15)), ct)
                    .ContinueWith(_ => { }, CancellationToken.None);
                if (message is not null) TrySeek(consumer, message);
            }
        }

        consumer.Close();
    }

    private void TrySeek(IConsumer<string, string> consumer, ConsumeResult<string, string> message)
    {
        try
        {
            consumer.Seek(message.TopicPartitionOffset);
        }
        catch (KafkaException e)
        {
            // Партицию уже отобрали — сообщение получит новый владелец.
            log.LogWarning(e, "Не удалось вернуться к сообщению");
        }
    }

    private async Task HandleAsync(ConsumeResult<string, string> message, CancellationToken ct)
    {
        var trace = message.Message.Headers.TryGetLastBytes("traceparent", out var raw)
            ? Encoding.UTF8.GetString(raw) : null;
        ActivityContext.TryParse(trace, null, out var parent);
        using var activity = OutboxPublisher.Source.StartActivity("refund-approved", ActivityKind.Consumer, parent);

        try
        {
            var body = JsonSerializer.Deserialize<RefundApproved>(message.Message.Value, JsonSerializerOptions.Web)
                       ?? throw new JsonException("Пустое сообщение");
            await using var scope = scopes.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<RegisterRefund>().ExecuteAsync(
                new(body.RefundId, body.BuyerId, body.SellerId, Money.Positive(body.AmountMinor, body.Currency)), ct);
        }
        catch (Exception e) when (e is JsonException or DomainException or ConflictException or OverflowException)
        {
            // Повтор не поможет. Без DLQ одно такое сообщение остановило бы партицию.
            log.LogError(e, "Сообщение ушло в DLQ");
            var headers = new Headers { { "error", Encoding.UTF8.GetBytes(e.Message) } };
            await producer.ProduceAsync(options.DeadLetterTopic,
                new Message<string, string> { Key = message.Message.Key, Value = message.Message.Value, Headers = headers }, ct);
        }
    }
}
