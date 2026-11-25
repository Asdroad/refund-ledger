using System.Net;
using System.Net.Http.Json;
using RefundLedger.Application;
using RefundLedger.Domain;

namespace RefundLedger.Infrastructure.Psp;

public sealed class HttpPspClient(HttpClient http) : IPspClient
{
    private sealed record Transfer(string Status, string? Reference, string? Reason);

    public async Task<PspResult> SendAsync(Payout payout, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/transfers")
        {
            Content = JsonContent.Create(new
            {
                amountMinor = payout.Amount.Minor, currency = payout.Amount.Currency.Code
            })
        };
        request.Headers.Add("Idempotency-Key", payout.Id.ToString());
        return await CallAsync(request, ct);
    }

    public async Task<PspResult> GetStatusAsync(Guid key, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/v1/transfers/{key}");
        return await CallAsync(request, ct);
    }

    public async Task<IReadOnlyList<PspOperation>> GetRegistryAsync(long afterSeq, int limit, CancellationToken ct) =>
        await http.GetFromJsonAsync<List<PspOperation>>($"/v1/registry?after={afterSeq}&limit={limit}", ct) ?? [];

    /// <summary>Всё, что не доказывает исход, считается неизвестным: ошибка в эту сторону безопасна.</summary>
    private async Task<PspResult> CallAsync(HttpRequestMessage request, CancellationToken ct)
    {
        try
        {
            using var response = await http.SendAsync(request, ct);
            switch (response.StatusCode)
            {
                case HttpStatusCode.TooManyRequests: return new PspResult.NotProcessed();
                case HttpStatusCode.NotFound: return new PspResult.NotFound();
                case HttpStatusCode.OK or HttpStatusCode.Accepted or HttpStatusCode.UnprocessableEntity: break;
                default: return new PspResult.Ambiguous();
            }

            var body = await response.Content.ReadFromJsonAsync<Transfer>(ct);
            return body switch
            {
                { Status: "succeeded", Reference: { } reference } => new PspResult.Accepted(reference),
                { Status: "declined" } => new PspResult.Declined(body.Reason ?? "unknown"),
                { Status: "processing" } => new PspResult.Processing(),
                _ => new PspResult.Ambiguous()
            };
        }
        catch (HttpRequestException e) when (e.HttpRequestError is HttpRequestError.ConnectionError
                                                 or HttpRequestError.NameResolutionError)
        {
            return new PspResult.NotProcessed();
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
            return new PspResult.Ambiguous();
        }
    }
}
