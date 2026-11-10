import { Link } from "react-router";

import { money, qty } from "../lib/format";
import { useBalances, useTickers } from "../lib/hooks";
import { Button, Empty, Panel, Spinner } from "./ui";

export function BalancesPanel({ onAddFunds, className }: { onAddFunds: () => void; className?: string }) {
  const balances = useBalances();
  const list = balances.data ?? [];
  const usd = list.find((b) => b.asset === "USD") ?? { asset: "USD", available: 0, reserved: 0 };
  const stocks = list.filter((b) => b.asset !== "USD" && b.available + b.reserved > 0);
  const ticks = useTickers(stocks.map((s) => s.asset));
  const stockValue = stocks.reduce((n, s) => n + (ticks[s.asset]?.last_price_cents ?? 0) * (s.available + s.reserved), 0);

  return (
    <Panel
      title="Portfolio"
      className={className}
      actions={
        <Button variant="ghost" size="sm" onClick={onAddFunds}>
          Add funds
        </Button>
      }
      bodyClassName="scroll-thin overflow-y-auto"
    >
      {balances.isLoading ? (
        <div className="flex h-24 items-center justify-center text-ink-3">
          <Spinner />
        </div>
      ) : (
        <div className="p-3">
          <p className="text-[11px] tracking-wide text-ink-3 uppercase">Total value</p>
          <p className="num mt-0.5 text-xl font-semibold">{money(usd.available + usd.reserved + stockValue)}</p>
          <dl className="mt-3 grid grid-cols-2 gap-2 text-xs">
            <div className="rounded-md bg-canvas px-2.5 py-2">
              <dt className="text-ink-3">Cash available</dt>
              <dd className="num mt-0.5 text-ink">{money(usd.available)}</dd>
            </div>
            <div className="rounded-md bg-canvas px-2.5 py-2" title="Held for open buy orders">
              <dt className="text-ink-3">In open orders</dt>
              <dd className="num mt-0.5 text-ink-2">{money(usd.reserved)}</dd>
            </div>
          </dl>
          <p className="mt-4 mb-1 text-[11px] font-medium text-ink-3">Holdings</p>
          {stocks.length === 0 ? (
            <Empty>No shares yet.</Empty>
          ) : (
            <table className="w-full text-xs">
              <tbody>
                {stocks.map((s) => {
                  const last = ticks[s.asset]?.last_price_cents;
                  return (
                    <tr key={s.asset} className="border-b border-line/60 last:border-0">
                      <td className="py-1.5 font-semibold">
                        <Link to={`/trade/${s.asset}`} className="hover:text-accent">
                          {s.asset}
                        </Link>
                      </td>
                      <td className="num py-1.5 text-right" title={s.reserved ? `${qty(s.reserved)} held for sell orders` : undefined}>
                        {qty(s.available + s.reserved)}
                        {s.reserved > 0 && <span className="text-ink-3"> ({qty(s.reserved)} held)</span>}
                      </td>
                      <td className="num py-1.5 text-right text-ink-2">
                        {last == null ? "—" : money(last * (s.available + s.reserved))}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      )}
    </Panel>
  );
}
