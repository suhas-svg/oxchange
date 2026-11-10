import { useState, type FormEvent } from "react";
import { Navigate, useLocation, useNavigate } from "react-router";

import { Button, Field, Logo, Segmented, inputClass } from "../components/ui";
import { ApiError, login, register } from "../lib/api";
import { useSession } from "../lib/hooks";

type Mode = "login" | "register";

export function LoginPage() {
  const session = useSession();
  const navigate = useNavigate();
  const location = useLocation();
  const [mode, setMode] = useState<Mode>("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const next = (location.state as { from?: string } | null)?.from ?? "/";
  if (session) return <Navigate to={next} replace />;

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    if (mode === "register" && password.length < 12) {
      setError("Use at least 12 characters for the password.");
      return;
    }
    setBusy(true);
    try {
      if (mode === "register") await register(email.trim(), password);
      await login(email.trim(), password);
      navigate(next, { replace: true });
    } catch (err) {
      if (err instanceof ApiError) {
        if (err.code === "ALREADY_EXISTS") setError("That email already has an account. Log in instead.");
        else if (err.status === 401) setError("Wrong email or password.");
        else if (err.code === "RATE_LIMITED") setError(`Too many attempts. Try again in ${err.retryAfter ?? 60} seconds.`);
        else setError(err.message);
      } else {
        setError("Something went wrong. Try again.");
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid min-h-full lg:grid-cols-[1fr_480px]">
      <aside className="relative hidden overflow-hidden border-r border-line bg-surface lg:block">
        <div
          className="absolute inset-0 opacity-[0.35]"
          style={{
            backgroundImage:
              "linear-gradient(var(--color-line) 1px, transparent 1px), linear-gradient(90deg, var(--color-line) 1px, transparent 1px)",
            backgroundSize: "48px 48px",
            maskImage: "radial-gradient(ellipse at 30% 40%, black 20%, transparent 70%)",
          }}
        />
        <div className="relative flex h-full flex-col justify-between p-12">
          <Logo />
          <div className="max-w-md">
            <p className="mb-3 text-xs font-medium tracking-[0.18em] text-accent uppercase">Simulated exchange</p>
            <h1 className="text-4xl leading-tight font-semibold tracking-tight">
              Trade twenty simulated stocks against real people, in real time.
            </h1>
            <ul className="mt-8 space-y-3 text-sm text-ink-2">
              {[
                "Price-time priority matching with a live order book",
                "Funds reserved before every order, settled after every trade",
                "Fills and cancels pushed to your browser as they happen",
              ].map((t) => (
                <li key={t} className="flex gap-3">
                  <span className="mt-1.5 size-1.5 shrink-0 rounded-full bg-up" />
                  {t}
                </li>
              ))}
            </ul>
          </div>
          <p className="text-xs text-ink-3">No real money or real companies. Balances are simulated.</p>
        </div>
      </aside>

      <main className="flex items-center justify-center p-6">
        <div className="w-full max-w-sm">
          <Logo className="mb-10 lg:hidden" />
          <h2 className="text-2xl font-semibold tracking-tight">{mode === "login" ? "Welcome back" : "Create your account"}</h2>
          <p className="mt-1.5 text-sm text-ink-3">
            {mode === "login" ? "Log in to your trading account." : "New accounts start empty; add simulated funds after."}
          </p>

          <Segmented
            className="mt-7"
            value={mode}
            onChange={(m) => {
              setMode(m);
              setError(null);
            }}
            options={[
              { value: "login", label: "Log in" },
              { value: "register", label: "Create account" },
            ]}
          />

          <form onSubmit={submit} className="mt-6 space-y-4">
            <Field label="Email">
              <input
                className={inputClass}
                type="email"
                autoComplete="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@example.com"
              />
            </Field>
            <Field label="Password" hint={mode === "register" ? "12 characters or more" : undefined}>
              <input
                className={inputClass}
                type="password"
                autoComplete={mode === "login" ? "current-password" : "new-password"}
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </Field>
            {error && (
              <p role="alert" className="rounded-md border border-down/30 bg-down-soft px-3 py-2 text-sm text-down">
                {error}
              </p>
            )}
            <Button type="submit" size="lg" className="w-full" busy={busy}>
              {mode === "login" ? "Log in" : "Create account"}
            </Button>
          </form>
        </div>
      </main>
    </div>
  );
}
