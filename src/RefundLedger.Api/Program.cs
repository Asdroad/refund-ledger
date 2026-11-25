using Microsoft.AspNetCore.Server.Kestrel.Core;
using Npgsql;
using RefundLedger.Api;
using RefundLedger.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders().AddJsonConsole();
builder.WebHost.ConfigureKestrel(k =>
{
    // Без TLS gRPC требует чистый HTTP/2, а пробам k8s нужен HTTP/1 — поэтому порта два.
    k.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http2);
    k.ListenAnyIP(8081, o => o.Protocols = HttpProtocols.Http1);
});
builder.Services.AddLedger(builder.Configuration).AddTelemetry(builder.Configuration, "refund-ledger-api");
builder.Services.AddGrpc(o => o.Interceptors.Add<ErrorInterceptor>());

var app = builder.Build();
app.MapGrpcService<LedgerGrpcService>();
app.MapGet("/health/live", () => Results.Ok());
app.MapGet("/health/ready", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var cmd = db.CreateCommand("select 1 from schema_migrations limit 1");
    await cmd.ExecuteScalarAsync(ct);
    return Results.Ok();
});
app.Run();
