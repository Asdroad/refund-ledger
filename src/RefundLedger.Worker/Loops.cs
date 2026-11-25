using RefundLedger.Application;
using RefundLedger.Infrastructure.Kafka;
using RefundLedger.Infrastructure.Postgres;
using RefundLedger.Infrastructure.Psp;
using RefundLedger.Infrastructure.Telemetry;

namespace RefundLedger.Worker;

public abstract class Loop(IServiceScopeFactory scopes, ILogger log, TimeSpan idle) : BackgroundService
{
    /// <returns>Сколько обработано. Больше нуля — следующий проход сразу.</returns>
    protected abstract Task<int> StepAsync(IServiceProvider services, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var done = 0;
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                done = await StepAsync(scope.ServiceProvider, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                log.LogError(e, "Сбой прохода");
            }

            if (done > 0) continue;
            try
            {
                await Task.Delay(idle, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}

public sealed class PayoutSenderLoop(IServiceScopeFactory s, ILogger<PayoutSenderLoop> l)
    : Loop(s, l, TimeSpan.FromMilliseconds(200))
{
    protected override Task<int> StepAsync(IServiceProvider p, CancellationToken ct) =>
        p.GetRequiredService<ProcessPayouts>().ExecuteAsync(PayoutQueue.ToSend, ct);
}

public sealed class PayoutResolverLoop(IServiceScopeFactory s, ILogger<PayoutResolverLoop> l)
    : Loop(s, l, TimeSpan.FromSeconds(1))
{
    protected override Task<int> StepAsync(IServiceProvider p, CancellationToken ct) =>
        p.GetRequiredService<ProcessPayouts>().ExecuteAsync(PayoutQueue.ToResolve, ct);
}

public sealed class OutboxLoop(IServiceScopeFactory s, ILogger<OutboxLoop> l)
    : Loop(s, l, TimeSpan.FromMilliseconds(200))
{
    protected override Task<int> StepAsync(IServiceProvider p, CancellationToken ct) =>
        p.GetRequiredService<OutboxPublisher>().PublishAsync(500, ct);
}

public sealed class RollUpLoop(IServiceScopeFactory s, ILogger<RollUpLoop> l)
    : Loop(s, l, TimeSpan.FromSeconds(1))
{
    protected override Task<int> StepAsync(IServiceProvider p, CancellationToken ct) =>
        p.GetRequiredService<Maintenance>().RollUpAsync(10_000, ct);
}

public sealed class RegistryLoop(IServiceScopeFactory s, ILogger<RegistryLoop> l)
    : Loop(s, l, TimeSpan.FromSeconds(30))
{
    protected override Task<int> StepAsync(IServiceProvider p, CancellationToken ct) =>
        p.GetRequiredService<RegistryReconciler>().RunAsync(1000, ct);
}

public sealed class PartitionLoop(IServiceScopeFactory s, ILogger<PartitionLoop> l)
    : Loop(s, l, TimeSpan.FromHours(6))
{
    protected override async Task<int> StepAsync(IServiceProvider p, CancellationToken ct)
    {
        await p.GetRequiredService<Maintenance>().EnsurePartitionsAsync(ct);
        return 0;
    }
}

public sealed class StatsLoop(IServiceScopeFactory s, ILogger<StatsLoop> l)
    : Loop(s, l, TimeSpan.FromSeconds(10))
{
    protected override async Task<int> StepAsync(IServiceProvider p, CancellationToken ct)
    {
        p.GetRequiredService<LedgerMetrics>().Update(await p.GetRequiredService<Maintenance>().GetStatsAsync(ct));
        return 0;
    }
}

public sealed class SelfCheckLoop(IServiceScopeFactory s, ILogger<SelfCheckLoop> l)
    : Loop(s, l, TimeSpan.FromMinutes(10))
{
    protected override async Task<int> StepAsync(IServiceProvider p, CancellationToken ct)
    {
        var (imbalance, drifted) = await p.GetRequiredService<Maintenance>().SelfCheckAsync(ct);
        p.GetRequiredService<LedgerMetrics>().UpdateSelfCheck(imbalance, drifted);
        if (imbalance != 0 || drifted != 0)
            l.LogCritical("Леджер не сходится: {Imbalance}, счетов {Drifted}", imbalance, drifted);
        return 0;
    }
}
