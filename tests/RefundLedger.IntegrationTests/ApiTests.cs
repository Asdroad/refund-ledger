extern alias api;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using RefundLedger.Application;
using RefundLedger.Contracts.V1;
using RefundLedger.Infrastructure.Caching;
using StackExchange.Redis;
using Money = RefundLedger.Contracts.V1.Money;
using RefundStatus = RefundLedger.Contracts.V1.RefundStatus;

namespace RefundLedger.IntegrationTests;

public class ApiTests(PostgresServer s) : LedgerTest(s)
{
    private LedgerService.LedgerServiceClient Client(string redis = "")
    {
        var factory = new WebApplicationFactory<api::Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("ConnectionStrings:Postgres", ConnectionString);
            b.UseSetting("ConnectionStrings:Redis", redis);
        });
        return new(GrpcChannel.ForAddress("http://localhost",
            new GrpcChannelOptions { HttpHandler = factory.Server.CreateHandler() }));
    }

    private static RegisterRefundRequest Request(long minor = 1_000) => new()
    {
        RefundId = Guid.NewGuid().ToString(), BuyerId = 7, SellerId = 1,
        Amount = new Money { Minor = minor, Currency = "RUB" }
    };

    [Fact]
    public async Task Полный_путь_через_gRPC()
    {
        var client = Client();
        await client.CreditSellerAsync(new()
            { OperationId = "op", SellerId = 1, Amount = new Money { Minor = 600, Currency = "RUB" } });
        var request = Request();
        var reply = await client.RegisterRefundAsync(request);
        Assert.Equal((600, 400), (reply.SellerPart.Minor, reply.PlatformPart.Minor));

        await Process.ExecuteAsync(PayoutQueue.ToSend, default);
        Assert.Equal(RefundStatus.Paid, (await client.GetRefundAsync(new() { RefundId = request.RefundId })).Status);
        Assert.Equal(0, (await client.GetBalanceAsync(new() { AccountCode = "seller:1:RUB" })).Balance.Minor);
        Assert.Equal(2, (await client.ListEntriesAsync(new() { AccountCode = "seller:1:RUB" })).Entries.Count);
    }

    [Fact]
    public async Task Ошибки_отдаются_понятными_кодами()
    {
        var client = Client();
        async Task<StatusCode> Code(Func<Task> call) => (await Assert.ThrowsAsync<RpcException>(call)).StatusCode;

        Assert.Equal(StatusCode.InvalidArgument, await Code(async () => await client.RegisterRefundAsync(Request(-1))));
        Assert.Equal(StatusCode.InvalidArgument, await Code(async () => await client.GetRefundAsync(new() { RefundId = "x" })));
        Assert.Equal(StatusCode.NotFound, await Code(async () => await client.GetRefundAsync(new() { RefundId = Guid.NewGuid().ToString() })));
        var request = Request();
        await client.RegisterRefundAsync(request);
        Assert.Equal(StatusCode.FailedPrecondition, await Code(async () => await client.RetryPayoutAsync(new() { RefundId = request.RefundId })));
        request.Amount.Minor++;
        Assert.Equal(StatusCode.FailedPrecondition, await Code(async () => await client.RegisterRefundAsync(request)));
    }

    [Fact]
    public async Task Redis_лежит_чтение_работает()
    {
        var client = Client("127.0.0.1:1");
        var request = Request();
        await client.RegisterRefundAsync(request);
        await Process.ExecuteAsync(PayoutQueue.ToSend, default);
        Assert.Equal(RefundStatus.Paid, (await client.GetRefundAsync(new() { RefundId = request.RefundId })).Status);
    }

    [SkippableByEnvFact("LEDGER_TEST_REDIS")]
    public async Task Кэшируется_только_оплаченный_возврат()
    {
        await using var redis = await ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("LEDGER_TEST_REDIS")!);
        var cache = new RedisPaidRefundCache(redis, NullLogger<RedisPaidRefundCache>.Instance);
        var get = new GetRefund(Reader, cache);
        var r = await RefundAsync(7, 1, 1_000);

        await get.ExecuteAsync(r.Id, default);
        Assert.Null(await cache.GetAsync(r.Id, default));

        await Process.ExecuteAsync(PayoutQueue.ToSend, default);
        await get.ExecuteAsync(r.Id, default);
        var cached = await cache.GetAsync(r.Id, default);
        Assert.Equal(1_000, cached!.Amount.Minor);
        Assert.Equal(Domain.RefundStatus.Paid, cached.Status);
    }
}

public sealed class SkippableByEnvFactAttribute : FactAttribute
{
    public SkippableByEnvFactAttribute(string variable)
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(variable))) Skip = $"Не задан {variable}";
    }
}
