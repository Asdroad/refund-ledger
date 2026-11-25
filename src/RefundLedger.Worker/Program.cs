using Npgsql;
using RefundLedger.Infrastructure;
using RefundLedger.Infrastructure.Kafka;
using RefundLedger.Infrastructure.Postgres;
using RefundLedger.Worker;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders().AddJsonConsole();
builder.Services.AddLedger(builder.Configuration)
    .AddPayoutProcessing(builder.Configuration)
    .AddKafka(builder.Configuration)
    .AddTelemetry(builder.Configuration, "refund-ledger-worker");

if (args.Contains("--migrate"))
{
    await using var migration = builder.Build();
    await migration.Services.GetRequiredService<Migrator>().RunAsync();
    return;
}

builder.Services.AddHostedService<RefundConsumer>();
builder.Services.AddHostedService<PayoutSenderLoop>();
builder.Services.AddHostedService<PayoutResolverLoop>();
builder.Services.AddHostedService<OutboxLoop>();
builder.Services.AddHostedService<RollUpLoop>();
builder.Services.AddHostedService<RegistryLoop>();
builder.Services.AddHostedService<PartitionLoop>();
builder.Services.AddHostedService<StatsLoop>();
builder.Services.AddHostedService<SelfCheckLoop>();

var app = builder.Build();
app.MapGet("/health/live", () => Results.Ok());
app.MapGet("/health/ready", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var cmd = db.CreateCommand("select 1 from schema_migrations limit 1");
    await cmd.ExecuteScalarAsync(ct);
    return Results.Ok();
});
app.Run();
