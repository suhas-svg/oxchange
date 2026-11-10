import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router";

import { logout } from "../lib/api";
import { money } from "../lib/format";
import { useBalances, useMarketStatus, usePrivateStatus, useSession } from "../lib/hooks";
import { describe, toast } from "../lib/notify";
import { privateStream } from "../lib/privateStream";
import { DepositDialog, openFunds } from "./DepositDialog";
import { NotificationBell } from "./NotificationBell";
import { Toasts } from "./Toasts";
import { Button, cx, Dot, Logo } from "./ui";

const SYMBOL_KEY = "oxchange.symbol.v1";

/** The stock last opened on the Trade page. */
export function lastSymbol(): string {
  try {
    return localStorage.getItem(SYMBOL_KEY) || "ACME";
  } catch {
    return "ACME";
  }
}

export function rememberSymbol(symbol: string) {
  try {
    localStorage.setItem(SYMBOL_KEY, symbol);
  } catch {
    // Only a convenience.
  }
}

/** Starts the private stream for the logged-in user and reacts to each new message. */
function usePrivateFeed(userId: string | undefined) {
  const qc = useQueryClient();
  useEffect(() => {
    if (!userId) return;
    void privateStream.start();
    const off = privateStream.onMessage((n, live) => {
      void qc.invalidateQueries({ queryKey: ["balances"] });
      if (n.type !== "account_update") void qc.invalidateQueries({ queryKey: ["orders"] });
      // Catch-up after a reconnect updates the lists quietly; only live news pops up.
      if (live) toast(describe(n));
    });
    return () => {
      off();
      privateStream.stop();
    };
  }, [userId, qc]);
}

function ConnectionStatus() {
  const market = useMarketStatus();
  const mine = usePrivateStatus();
  const ok = market === "live" && mine === "live";
  const label = ok ? "Live" : market === "reconnecting" || mine === "reconnecting" ? "Reconnecting" : "Connecting";
  return (
    <span
      className="hidden items-center gap-2 rounded-md px-2 py-1 text-xs text-ink-3 sm:inline-flex"
      title={`Market data: ${market}. Your updates: ${mine}.`}
    >
      <Dot tone={ok ? "up" : "warn"} />
      {label}
    </span>
  );
}

function UserMenu({ email }: { email: string }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const navigate = useNavigate();
  const qc = useQueryClient();
  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => ref.current && !ref.current.contains(e.target as Node) && setOpen(false);
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [open]);
  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        className="grid size-8 place-items-center rounded-full bg-raised text-xs font-semibold text-ink-2 uppercase ring-1 ring-line-strong hover:text-ink"
        aria-label="Account menu"
      >
        {email.slice(0, 1) || "?"}
      </button>
      {open && (
        <div className="absolute top-10 right-0 z-40 w-60 overflow-hidden rounded-lg border border-line-strong bg-raised py-1 shadow-2xl shadow-black/50">
          <p className="truncate border-b border-line px-3 py-2 text-xs text-ink-3">{email}</p>
          <button
            className="w-full px-3 py-2 text-left text-sm text-ink-2 hover:bg-hover hover:text-ink"
            onClick={async () => {
              setOpen(false);
              await logout().catch(() => undefined);
              qc.clear();
              navigate("/login", { replace: true });
            }}
          >
            Log out
          </button>
        </div>
      )}
    </div>
  );
}

export function AppShell() {
  const session = useSession();
  const balances = useBalances();
  const { pathname } = useLocation();
  usePrivateFeed(session?.userId || session?.email);

  const usd = balances.data?.find((b) => b.asset === "USD");
  const tab = ({ isActive }: { isActive: boolean }) =>
    cx("rounded-md px-3 py-1.5 text-sm font-medium transition", isActive ? "bg-raised text-ink" : "text-ink-3 hover:text-ink-2");

  return (
    <div className="flex h-full flex-col">
      <header className="flex h-14 shrink-0 items-center gap-2 border-b border-line bg-surface px-3 sm:gap-4 sm:px-4">
        <NavLink to="/" className="mr-1 shrink-0">
          <Logo />
        </NavLink>
        <nav className="flex items-center gap-1">
          <NavLink to="/" end className={tab}>
            Markets
          </NavLink>
          <NavLink to={`/trade/${lastSymbol()}`} className={() => tab({ isActive: pathname.startsWith("/trade") })}>
            Trade
          </NavLink>
        </nav>
        <div className="ml-auto flex items-center gap-1.5 sm:gap-3">
          <ConnectionStatus />
          <button
            onClick={() => openFunds()}
            className="hidden flex-col items-end rounded-md px-2 py-1 text-right hover:bg-hover md:flex"
            title="Cash you can trade with"
          >
            <span className="text-[10px] tracking-wide text-ink-3 uppercase">Cash</span>
            <span className="num text-[13px] leading-tight">{balances.isLoading ? "…" : money(usd?.available ?? 0)}</span>
          </button>
          <Button size="sm" onClick={() => openFunds()}>
            Add funds
          </Button>
          <NotificationBell />
          {session && <UserMenu email={session.email} />}
        </div>
      </header>
      <main className="min-h-0 flex-1">
        <Outlet />
      </main>
      <DepositDialog />
      <Toasts />
    </div>
  );
}
