import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Navigate, Route, Routes, useLocation } from "react-router";

import "./index.css";
import { AppShell } from "./components/AppShell";
import { ApiError } from "./lib/api";
import { useSession } from "./lib/hooks";
import { LoginPage } from "./pages/Login";
import { MarketsPage } from "./pages/Markets";
import { TradePage } from "./pages/Trade";

const queries = new QueryClient({
  defaultOptions: {
    queries: {
      // A 4xx will not get better by asking again.
      retry: (n, e) => n < 2 && !(e instanceof ApiError && e.status >= 400 && e.status < 500),
      refetchOnWindowFocus: false,
    },
  },
});

function RequireLogin({ children }: { children: ReactNode }) {
  const session = useSession();
  const location = useLocation();
  if (!session) return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  return children;
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queries}>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            element={
              <RequireLogin>
                <AppShell />
              </RequireLogin>
            }
          >
            <Route index element={<MarketsPage />} />
            <Route path="trade/:symbol" element={<TradePage />} />
            <Route path="trade" element={<Navigate to="/" replace />} />
          </Route>
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
