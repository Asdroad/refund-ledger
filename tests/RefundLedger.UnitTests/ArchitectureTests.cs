using NetArchTest.Rules;
using RefundLedger.Domain;

namespace RefundLedger.UnitTests;

public class ArchitectureTests
{
    [Fact]
    public void Домен_ни_от_чего_не_зависит() => AssertClean(
        Types.InAssembly(typeof(Refund).Assembly).ShouldNot().HaveDependencyOnAny(
            "RefundLedger.Application", "RefundLedger.Infrastructure", "Npgsql", "Dapper",
            "Confluent.Kafka", "StackExchange.Redis", "Grpc", "Microsoft").GetResult());

    [Fact]
    public void Сценарии_не_знают_об_инфраструктуре() => AssertClean(
        Types.InAssembly(typeof(Application.RegisterRefund).Assembly).ShouldNot().HaveDependencyOnAny(
            "RefundLedger.Infrastructure", "Npgsql", "Dapper", "Confluent.Kafka",
            "StackExchange.Redis", "Grpc", "Microsoft.AspNetCore").GetResult());

    private static void AssertClean(TestResult r) =>
        Assert.True(r.IsSuccessful, string.Join(", ", r.FailingTypeNames ?? []));
}
