using System.Text.Json;
using Microsoft.Extensions.Logging;
using RefundLedger.Application;
using RefundLedger.Domain;
using StackExchange.Redis;

namespace RefundLedger.Infrastructure.Caching;

/// <summary>Redis здесь ускорение, а не источник истины: его отказ не ломает чтение.</summary>
public sealed class RedisPaidRefundCache(IConnectionMultiplexer redis, ILogger<RedisPaidRefundCache> log)
    : IPaidRefundCache
{
    private sealed record Dto(long Buyer, long Seller, long Amount, long SellerPart, string Currency, int Version);

    public async Task<Refund?> GetAsync(Guid id, CancellationToken ct)
    {
        try
        {
            var value = await redis.GetDatabase().StringGetAsync(Key(id)).WaitAsync(ct);
            if (value.IsNullOrEmpty) return null;
            var d = JsonSerializer.Deserialize<Dto>(value.ToString())!;
            var c = new Currency(d.Currency);
            return new Refund(id, d.Buyer, d.Seller, new(d.Amount, c), new(d.SellerPart, c), RefundStatus.Paid, d.Version);
        }
        catch (Exception e) when (e is RedisException or TimeoutException or JsonException)
        {
            log.LogWarning(e, "Кэш недоступен, читаем из БД");
            return null;
        }
    }

    public async Task SetAsync(Refund r, CancellationToken ct)
    {
        try
        {
            var json = JsonSerializer.Serialize(new Dto(r.BuyerId, r.SellerId, r.Amount.Minor, r.SellerPart.Minor,
                r.Amount.Currency.Code, r.Version));
            await redis.GetDatabase().StringSetAsync(Key(r.Id), json, TimeSpan.FromHours(6)).WaitAsync(ct);
        }
        catch (Exception e) when (e is RedisException or TimeoutException)
        {
            log.LogWarning(e, "Не удалось записать в кэш");
        }
    }

    private static string Key(Guid id) => $"refund:paid:{id}";
}

public sealed class NoCache : IPaidRefundCache
{
    public Task<Refund?> GetAsync(Guid id, CancellationToken ct) => Task.FromResult<Refund?>(null);
    public Task SetAsync(Refund refund, CancellationToken ct) => Task.CompletedTask;
}
