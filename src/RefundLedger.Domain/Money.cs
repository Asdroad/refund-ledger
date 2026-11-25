namespace RefundLedger.Domain;

public class DomainException(string message) : Exception(message);

public sealed class InvalidTransitionException(string message) : DomainException(message);

public readonly record struct Currency
{
    public string Code { get; }

    public Currency(string code)
    {
        if (code is not { Length: 3 } || !code.All(c => c is >= 'A' and <= 'Z'))
            throw new DomainException($"Некорректная валюта: '{code}'");
        Code = code;
    }

    public override string ToString() => Code;
}

/// <summary>Сумма в минимальных единицах. Знак: плюс — кредит, минус — дебет.</summary>
public readonly record struct Money(long Minor, Currency Currency)
{
    public static Money operator -(Money a) => a with { Minor = checked(-a.Minor) };

    public static Money operator +(Money a, Money b)
    {
        if (a.Currency != b.Currency)
            throw new DomainException($"Нельзя сложить {a.Currency} и {b.Currency}");
        return a with { Minor = checked(a.Minor + b.Minor) };
    }

    public static Money operator -(Money a, Money b) => a + -b;

    public static Money Positive(long minor, string currency) =>
        minor > 0
            ? new Money(minor, new Currency(currency))
            : throw new DomainException("Сумма должна быть положительной");
}
