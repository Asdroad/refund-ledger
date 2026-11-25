using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using RefundLedger.Application;
using RefundLedger.Infrastructure.Caching;
using RefundLedger.Infrastructure.Kafka;
using RefundLedger.Infrastructure.Postgres;
using RefundLedger.Infrastructure.Psp;
using RefundLedger.Infrastructure.Telemetry;
using StackExchange.Redis;

namespace RefundLedger.Infrastructure;

public static class Registration
{
    public static IServiceCollection AddLedger(this IServiceCollection s, IConfiguration cfg)
    {
        var pg = cfg.GetConnectionString("Postgres")
                 ?? throw new InvalidOperationException("Не задан ConnectionStrings:Postgres");
        s.Configure<LedgerOptions>(cfg.GetSection("Ledger"));
        s.AddSingleton(NpgsqlDataSource.Create(pg));
        s.AddSingleton(_ => new AccountDirectoryDb(pg));
        s.AddSingleton<AccountDirectory>();
        s.AddSingleton<ILedgerSessionFactory, LedgerSessionFactory>();
        s.AddSingleton<ILedgerReader, LedgerReader>();
        s.AddSingleton<Migrator>();
        s.AddSingleton<Maintenance>();
        s.AddSingleton(TimeProvider.System);
        s.AddTransient<RegisterRefund>();
        s.AddTransient<CreditSeller>();
        s.AddTransient<RetryPayout>();
        s.AddTransient<GetRefund>();

        if (cfg.GetConnectionString("Redis") is { Length: > 0 } redis)
        {
            s.AddSingleton<IConnectionMultiplexer>(_ =>
            {
                var o = ConfigurationOptions.Parse(redis);
                o.AbortOnConnectFail = false;
                o.ConnectTimeout = o.AsyncTimeout = o.SyncTimeout = 300;
                // Без очереди: при обрыве сразу идём в БД, а не копим запросы.
                o.BacklogPolicy = BacklogPolicy.FailFast;
                return ConnectionMultiplexer.Connect(o);
            });
            s.AddSingleton<IPaidRefundCache, RedisPaidRefundCache>();
        }
        else
        {
            s.AddSingleton<IPaidRefundCache, NoCache>();
        }

        return s;
    }

    public static IServiceCollection AddPayoutProcessing(this IServiceCollection s, IConfiguration cfg)
    {
        s.AddOptions<PayoutOptions>().Bind(cfg.GetSection("Payouts"))
            // Иначе аренда кончится раньше ответа, и каждую медленную выплату перехватят.
            .Validate(o => o.PspTimeout < o.Lease, "PspTimeout должен быть меньше Lease")
            .ValidateOnStart();
        s.AddSingleton(p => p.GetRequiredService<IOptions<PayoutOptions>>().Value);
        s.AddHttpClient<IPspClient, HttpPspClient>((p, http) =>
        {
            http.BaseAddress = new Uri(cfg["Psp:BaseUrl"] ?? "http://localhost:5080");
            http.Timeout = p.GetRequiredService<PayoutOptions>().PspTimeout;
        });
        s.AddTransient<ProcessPayouts>();
        s.AddTransient<RegistryReconciler>();
        return s;
    }

    public static IServiceCollection AddKafka(this IServiceCollection s, IConfiguration cfg)
    {
        var options = cfg.GetSection("Kafka").Get<KafkaOptions>() ?? new KafkaOptions();
        s.AddSingleton(options);
        s.AddSingleton(_ => new ProducerBuilder<string, string>(
            OutboxPublisher.ProducerConfig(options.BootstrapServers)).Build());
        s.AddSingleton<OutboxPublisher>();
        return s;
    }

    public static IServiceCollection AddTelemetry(this IServiceCollection s, IConfiguration cfg, string service)
    {
        s.AddSingleton<LedgerMetrics>();
        var otel = s.AddOpenTelemetry().ConfigureResource(r => r.AddService(service));
        var traces = cfg["Otlp:Traces"];
        var metrics = cfg["Otlp:Metrics"];

        otel.WithTracing(t =>
        {
            t.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddNpgsql()
                .AddSource(ProcessPayouts.Source.Name, OutboxPublisher.Source.Name);
            if (traces is { Length: > 0 }) t.AddOtlpExporter(o => o.Endpoint = new Uri(traces));
        });
        otel.WithMetrics(m =>
        {
            m.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation()
                .AddMeter(LedgerMetrics.MeterName, "Npgsql");
            if (metrics is { Length: > 0 })
                m.AddOtlpExporter(o =>
                {
                    o.Endpoint = new Uri(metrics);
                    o.Protocol = OtlpExportProtocol.HttpProtobuf;
                });
        });
        return s;
    }
}
