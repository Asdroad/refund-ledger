using System.Collections.Concurrent;

var app = WebApplication.CreateBuilder(args).Build();
var transfers = new ConcurrentDictionary<Guid, Transfer>();
var registry = new List<Operation>();
var gate = new Lock();
var mode = app.Configuration["Mode"] ?? "ok";
var delay = TimeSpan.FromMilliseconds(app.Configuration.GetValue("DelayMs", 0));

// Режимы: ok, decline, throttle, error_before, error_after, hang_after, chaos.
app.MapPost("/v1/transfers", async (HttpContext http, TransferRequest body) =>
{
    if (!Guid.TryParse(http.Request.Headers["Idempotency-Key"], out var key)) return Results.BadRequest();
    var current = mode == "chaos" ? Chaos() : mode;

    if (current == "throttle") return Results.StatusCode(429);
    if (current == "error_before") return Results.StatusCode(500);
    if (delay > TimeSpan.Zero) await Task.Delay(delay);

    Transfer result;
    lock (gate)
    {
        if (!transfers.TryGetValue(key, out result!))
        {
            result = current == "decline"
                ? new("declined", null, "card_closed")
                : new("succeeded", $"psp-{registry.Count + 1}", null);
            transfers[key] = result;
            registry.Add(new(registry.Count + 1, key, body.AmountMinor, body.Currency, result.Status));
        }
    }

    // Деньги ушли, но клиент об этом не узнал.
    if (current == "error_after") return Results.StatusCode(502);
    if (current == "hang_after") await Task.Delay(TimeSpan.FromSeconds(30), http.RequestAborted);

    return result.Status == "declined" ? Results.UnprocessableEntity(result) : Results.Ok(result);
});

app.MapGet("/v1/transfers/{key:guid}", (Guid key) =>
    transfers.TryGetValue(key, out var t) ? Results.Ok(t) : Results.NotFound());

app.MapGet("/v1/registry", (long after, int limit) =>
{
    lock (gate) return Results.Ok(registry.Where(o => o.Seq > after).Take(limit).ToList());
});

app.MapPost("/admin/mode/{value}", (string value) =>
{
    mode = value;
    return Results.Ok();
});

app.MapGet("/health/live", () => Results.Ok());
app.Run();

static string Chaos() => Random.Shared.Next(100) switch
{
    < 10 => "error_after", < 15 => "hang_after", < 25 => "throttle", < 35 => "error_before", < 40 => "decline", _ => "ok"
};

internal sealed record TransferRequest(long AmountMinor, string Currency);

internal sealed record Transfer(string Status, string? Reference, string? Reason);

internal sealed record Operation(long Seq, Guid Key, long AmountMinor, string Currency, string Status);

public partial class Program;
