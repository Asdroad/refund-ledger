using Grpc.Core;
using Grpc.Core.Interceptors;
using RefundLedger.Application;
using RefundLedger.Contracts.V1;
using RefundLedger.Domain;
using Money = RefundLedger.Domain.Money;
using MoneyDto = RefundLedger.Contracts.V1.Money;
using RefundStatusDto = RefundLedger.Contracts.V1.RefundStatus;

namespace RefundLedger.Api;

public sealed class LedgerGrpcService(
    RegisterRefund registerRefund, GetRefund getRefund, RetryPayout retryPayout, CreditSeller creditSeller,
    ILedgerReader reader) : LedgerService.LedgerServiceBase
{
    public override async Task<RefundReply> RegisterRefund(RegisterRefundRequest r, ServerCallContext ctx) =>
        Map(await registerRefund.ExecuteAsync(
            new(Id(r.RefundId), r.BuyerId, r.SellerId, Amount(r.Amount)), ctx.CancellationToken));

    public override async Task<RefundReply> GetRefund(GetRefundRequest r, ServerCallContext ctx) =>
        Map(await getRefund.ExecuteAsync(Id(r.RefundId), ctx.CancellationToken));

    public override async Task<RefundReply> RetryPayout(RetryPayoutRequest r, ServerCallContext ctx) =>
        Map(await retryPayout.ExecuteAsync(Id(r.RefundId), ctx.CancellationToken));

    public override async Task<CreditSellerReply> CreditSeller(CreditSellerRequest r, ServerCallContext ctx) =>
        new() { Duplicate = await creditSeller.ExecuteAsync(r.OperationId, r.SellerId, Amount(r.Amount), ctx.CancellationToken) };

    public override async Task<BalanceReply> GetBalance(GetBalanceRequest r, ServerCallContext ctx)
    {
        var minor = await reader.GetBalanceAsync(r.AccountCode, ctx.CancellationToken)
                    ?? throw new NotFoundException($"Счёт {r.AccountCode} не найден");
        return new() { Balance = new() { Minor = minor, Currency = r.AccountCode[^3..] } };
    }

    public override async Task<ListEntriesReply> ListEntries(ListEntriesRequest r, ServerCallContext ctx)
    {
        var size = r.PageSize is > 0 and <= 500 ? r.PageSize : 100;
        var rows = await reader.ListEntriesAsync(r.AccountCode, r.Cursor > 0 ? r.Cursor : null, size, ctx.CancellationToken);
        var reply = new ListEntriesReply { NextCursor = rows.Count == size ? rows[^1].Id : 0 };
        reply.Entries.AddRange(rows.Select(e => new Entry
        {
            Id = e.Id, PostingKey = e.PostingKey, BookedAtUnixMs = e.BookedAt.ToUnixTimeMilliseconds(),
            Amount = new() { Minor = e.AmountMinor, Currency = e.Currency }
        }));
        return reply;
    }

    private static Guid Id(string value) =>
        Guid.TryParse(value, out var id) ? id : throw new DomainException("Некорректный идентификатор возврата");

    private static Money Amount(MoneyDto? m) =>
        m is null ? throw new DomainException("Не указана сумма") : Money.Positive(m.Minor, m.Currency);

    private static RefundReply Map(Refund r) => new()
    {
        RefundId = r.Id.ToString(), Status = (RefundStatusDto)(int)r.Status,
        Amount = Map(r.Amount), SellerPart = Map(r.SellerPart), PlatformPart = Map(r.PlatformPart)
    };

    private static MoneyDto Map(Money m) => new() { Minor = m.Minor, Currency = m.Currency.Code };
}

public sealed class ErrorInterceptor(ILogger<ErrorInterceptor> log) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (Exception e) when (e is not RpcException and not OperationCanceledException)
        {
            var code = e switch
            {
                InvalidTransitionException or ConflictException or InsufficientFundsException => StatusCode.FailedPrecondition,
                NotFoundException => StatusCode.NotFound,
                DomainException or OverflowException => StatusCode.InvalidArgument,
                _ => StatusCode.Internal
            };
            if (code == StatusCode.Internal) log.LogError(e, "Сбой {Method}", context.Method);
            // Текст внутренней ошибки наружу не отдаём: в нём бывают детали БД.
            throw new RpcException(new Status(code, code == StatusCode.Internal ? "Внутренняя ошибка" : e.Message));
        }
    }
}
