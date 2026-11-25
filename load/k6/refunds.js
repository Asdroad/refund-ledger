// k6 run load/k6/refunds.js
import grpc from 'k6/net/grpc';
import exec from 'k6/execution';
import { check } from 'k6';

const client = new grpc.Client();
client.load(['../../proto'], 'ledger/v1/ledger.proto');

export const options = {
  scenarios: {
    refunds: { executor: 'ramping-arrival-rate', startRate: 20, timeUnit: '1s', preAllocatedVUs: 100, maxVUs: 300,
      stages: [{ target: 200, duration: '30s' }, { target: 200, duration: '1m' }] },
  },
  thresholds: { grpc_req_duration: ['p(99)<200'], checks: ['rate>0.999'] },
};

const uuid = () => 'xxxxxxxx-xxxx-4xxx-8xxx-xxxxxxxxxxxx'.replace(/x/g, () => Math.floor(Math.random() * 16).toString(16));

export default function () {
  if (exec.vu.iterationInInstance === 0) client.connect('localhost:5001', { plaintext: true });
  const request = {
    refund_id: uuid(),
    buyer_id: 1 + Math.floor(Math.random() * 100000),
    seller_id: 1 + Math.floor(Math.random() * 2000),
    amount: { minor: 100 + Math.floor(Math.random() * 500000), currency: 'RUB' },
  };
  const first = client.invoke('ledger.v1.LedgerService/RegisterRefund', request);
  check(first, { ok: (r) => r.status === grpc.StatusOK });
  // Каждый десятый запрос клиент «повторяет», как после таймаута.
  if (Math.random() < 0.1) {
    const again = client.invoke('ledger.v1.LedgerService/RegisterRefund', request);
    check(again, { 'повтор безвреден': (r) => r.status === grpc.StatusOK });
  }
}
