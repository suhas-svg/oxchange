// The login session, shared by every tab of this browser.
//
// Refresh tokens work once, and reusing one revokes all of the user's
// refresh tokens (spec: "Refresh rotation"). Two tabs refreshing with the
// same token at the same moment would log the user out everywhere, so the
// session lives in localStorage and refreshes run one at a time across tabs
// (Web Locks). A tab that waited for the lock finds the new tokens already
// stored and uses them instead of refreshing again.
//
// Trade-off: tokens in localStorage are readable by any script on this
// origin. The page loads no third-party scripts, and access tokens last 15
// minutes.

import type { Tokens } from "./types";

export interface Session {
  userId: string;
  email: string;
  accessToken: string;
  /** Unix ms */
  accessExpiresAt: number;
  refreshToken: string;
  refreshExpiresAt: number;
}

const KEY = "oxchange.session.v1";
/** Refresh this long before the access token expires. */
const EARLY_MS = 30_000;

type Listener = (s: Session | null) => void;
const listeners = new Set<Listener>();

function load(): Session | null {
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return null;
    const s = JSON.parse(raw) as Session;
    return s.refreshExpiresAt > Date.now() ? s : null;
  } catch {
    return null;
  }
}

let current: Session | null = load();

function emit() {
  for (const l of listeners) l(current);
}

if (typeof window !== "undefined") {
  // Another tab logged in, refreshed, or logged out.
  window.addEventListener("storage", (e) => {
    if (e.key === KEY) {
      current = load();
      emit();
    }
  });
}

export function getSession(): Session | null {
  return current;
}

export function subscribeSession(l: Listener): () => void {
  listeners.add(l);
  return () => listeners.delete(l);
}

export function setSession(s: Session | null) {
  current = s;
  try {
    if (s) localStorage.setItem(KEY, JSON.stringify(s));
    else localStorage.removeItem(KEY);
  } catch {
    // Private mode or storage full: the session still works in this tab.
  }
  emit();
}

export function sessionFrom(tokens: Tokens, userId: string, email: string): Session {
  return {
    userId,
    email,
    accessToken: tokens.access_token,
    accessExpiresAt: Date.parse(tokens.access_expires_at),
    refreshToken: tokens.refresh_token,
    refreshExpiresAt: Date.parse(tokens.refresh_expires_at),
  };
}

function fresh(s: Session | null): s is Session {
  return !!s && s.accessExpiresAt - EARLY_MS > Date.now();
}

async function withLock<T>(fn: () => Promise<T>): Promise<T> {
  if (typeof navigator !== "undefined" && navigator.locks) {
    return navigator.locks.request("oxchange-refresh", fn);
  }
  return fn();
}

let inflight: Promise<string | null> | null = null;

/**
 * A usable access token, refreshing first if it is about to expire.
 * `force` refreshes even if the token looks fresh (after a 401).
 * Returns null when the user must log in again.
 */
export function accessToken(force = false): Promise<string | null> {
  const s = current;
  if (!s) return Promise.resolve(null);
  if (!force && fresh(s)) return Promise.resolve(s.accessToken);
  if (!inflight) {
    const stale = s.accessToken;
    inflight = withLock(async () => {
      // Another tab may have refreshed while this one waited.
      const stored = load();
      if (!stored) {
        setSession(null);
        return null;
      }
      if (stored.accessToken !== stale && fresh(stored)) {
        current = stored;
        emit();
        return stored.accessToken;
      }
      const r = await fetch("/v1/auth/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: stored.refreshToken }),
      });
      if (r.status === 401) {
        setSession(null);
        return null;
      }
      if (!r.ok) {
        // A temporary failure: keep the session, let the caller retry.
        throw new Error(`refresh failed (${r.status})`);
      }
      const tokens = (await r.json()) as Tokens;
      const next = sessionFrom(tokens, stored.userId, stored.email);
      setSession(next);
      return next.accessToken;
    }).finally(() => {
      inflight = null;
    });
  }
  return inflight;
}
