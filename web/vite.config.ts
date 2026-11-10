import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// In development the API Gateway runs at http://localhost:8000 (docker
// compose); the dev server forwards the API and both WebSockets to it, so
// the browser sees one origin, exactly as behind the production web server.
const gateway = process.env.GATEWAY_URL ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    rollupOptions: {
      output: {
        // The chart library is most of the bundle and changes rarely; its own
        // chunk stays cached across app releases.
        manualChunks: { charts: ["lightweight-charts"], react: ["react", "react-dom", "react-router", "@tanstack/react-query"] },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/v1": { target: gateway, changeOrigin: true },
      "/ws": { target: gateway, ws: true, changeOrigin: true },
    },
  },
  test: {
    environment: "node",
  },
});
