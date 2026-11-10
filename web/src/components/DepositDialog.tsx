import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState, useSyncExternalStore, type FormEvent } from "react";

import { ApiError, deposit, newId, withdraw } from "../lib/api";
import { money, parseDollars, parseShares, qty } from "../lib/format";
import { useBalances, useSymbols } from "../lib/hooks";
import { Button, Field, Modal, Segmented, inputClass } from "./ui";

// Any component can open the dialog, optionally on a stock.
let state: { open: boolean; asset: string } = { open: false, asset: "USD" };
const listeners = new Set<() => void>();
function set(s: typeof state) {
  state = s;
  for (const l of listeners) l();
}
export const openFunds = (asset = "USD") => set({ open: true, asset });
const closeFunds = () => set({ ...state, open: false });

const PRESETS = [1_000_00, 10_000_00, 100_000_00];

export function DepositDialog() {
  const { open, asset: initial } = useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => state,
  );
  const qc = useQueryClient();
  const balances = useBalances();
  const symbols = useSymbols();
  const [mode, setMode] = useState<"deposit" | "withdraw">("deposit");
  const [asset, setAsset] = useState("USD");
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  // One key per attempt, so pressing again after a timeout cannot move money twice.
  const key = useRef(newId("web"));

  useEffect(() => {
    if (open) {
      setAsset(initial);
      setText("");
      setError(null);
      key.current = newId("web");
    }
  }, [open, initial]);

  const isCash = asset === "USD";
  const have = balances.data?.find((b) => b.asset === asset)?.available ?? 0;
  const m = useMutation({
    mutationFn: (amount: number) =>
      mode === "deposit" ? deposit(asset, amount, key.current) : withdraw(asset, amount, key.current),
    // The account-update notification confirms it with a toast.
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["balances"] });
      key.current = newId("web");
      closeFunds();
    },
    onError: (e) => {
      if (e instanceof ApiError && e.temporary) setError("The exchange is busy. Press the button again; it won't count twice.");
      else if (e instanceof ApiError && e.code === "INSUFFICIENT_FUNDS") setError("That's more than you have available.");
      else setError(e instanceof Error ? e.message : "Something went wrong.");
    },
  });

  function submit(e: FormEvent) {
    e.preventDefault();
    const amount = isCash ? parseDollars(text) : parseShares(text);
    if (amount == null || amount <= 0) {
      setError(isCash ? "Enter an amount like 2500 or 2500.50." : "Enter a whole number of shares.");
      return;
    }
    setError(null);
    m.mutate(amount);
  }

  function reset() {
    setText("");
    setError(null);
    key.current = newId("web");
  }

  return (
    <Modal open={open} onClose={closeFunds} title="Simulated funds">
      <Segmented
        value={mode}
        onChange={(v) => {
          setMode(v);
          reset();
        }}
        options={[
          { value: "deposit", label: "Add" },
          { value: "withdraw", label: "Withdraw" },
        ]}
      />
      <form onSubmit={submit} className="mt-5 space-y-4">
        <Field label="Asset">
          <select
            className={inputClass}
            value={asset}
            onChange={(e) => {
              setAsset(e.target.value);
              reset();
            }}
          >
            <option value="USD">Cash (USD)</option>
            {(symbols.data ?? []).map((s) => (
              <option key={s.symbol} value={s.symbol}>
                {s.symbol} shares · {s.name}
              </option>
            ))}
          </select>
        </Field>
        <Field
          label={isCash ? "Amount (USD)" : "Shares"}
          hint={<span className="num">Available {isCash ? money(have) : qty(have)}</span>}
        >
          <div className="relative">
            {isCash && <span className="pointer-events-none absolute top-1/2 left-3 -translate-y-1/2 text-sm text-ink-3">$</span>}
            <input
              autoFocus
              inputMode={isCash ? "decimal" : "numeric"}
              className={`${inputClass} num ${isCash ? "pl-7" : ""}`}
              placeholder={isCash ? "0.00" : "0"}
              value={text}
              onChange={(e) => setText(e.target.value)}
            />
          </div>
        </Field>
        {mode === "deposit" && (
          <div className="flex gap-2">
            {(isCash ? PRESETS : [100, 1_000, 10_000]).map((p) => (
              <Button
                key={p}
                type="button"
                variant="outline"
                size="sm"
                className="flex-1"
                onClick={() => setText(String(isCash ? p / 100 : p))}
              >
                {isCash ? money(p).replace(".00", "") : `${qty(p)} sh`}
              </Button>
            ))}
          </div>
        )}
        {error && <p className="text-sm text-down">{error}</p>}
        <Button type="submit" className="w-full" size="lg" busy={m.isPending}>
          {mode === "deposit" ? "Add" : "Withdraw"} {isCash ? "cash" : `${asset} shares`}
        </Button>
        <p className="text-center text-xs text-ink-3">Everything here is simulated. Nothing is charged.</p>
      </form>
    </Modal>
  );
}
