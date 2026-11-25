using System.Diagnostics.Metrics;
using RefundLedger.Infrastructure.Postgres;

namespace RefundLedger.Infrastructure.Telemetry;

public sealed class LedgerMetrics
{
    public const string MeterName = "RefundLedger";

    private LedgerStats _stats = new(0, 0, 0, 0, 0, 0, 0);
    private long _imbalance, _drifted;

    public LedgerMetrics(IMeterFactory factory)
    {
        var m = factory.Create(MeterName);
        m.CreateObservableGauge("ledger_imbalance_minor", () => _imbalance, description: "Всегда 0");
        m.CreateObservableGauge("ledger_drifted_accounts", () => _drifted, description: "Всегда 0");
        m.CreateObservableGauge("ledger_outbox_rows", () => _stats.Outbox);
        m.CreateObservableGauge("ledger_outbox_age_seconds", () => _stats.OutboxAgeSeconds);
        m.CreateObservableGauge("ledger_balance_deltas", () => _stats.Deltas);
        m.CreateObservableGauge("ledger_payouts_pending", () => _stats.PayoutsPending);
        m.CreateObservableGauge("ledger_payouts_in_flight", () => _stats.PayoutsInFlight);
        m.CreateObservableGauge("ledger_oldest_unknown_seconds", () => _stats.OldestUnknownSeconds);
        m.CreateObservableGauge("ledger_reconciliation_issues", () => _stats.ReconciliationIssues);
    }

    public void Update(LedgerStats stats) => _stats = stats;
    public void UpdateSelfCheck(long imbalance, long drifted) => (_imbalance, _drifted) = (imbalance, drifted);
}
