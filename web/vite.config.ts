import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Dev server config:
//   - Proxy /v1/* to the FastAPI service so the browser sees a single
//     origin (matches the prod nginx setup) and we don't need CORS on
//     the API side.
//   - Default API target is localhost:8000; override with VITE_API_TARGET
//     for talking to a remote stack.
const API_TARGET = process.env.VITE_API_TARGET ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  server: {
    port: 5173,
    proxy: {
      "/v1": {
        target: API_TARGET,
        changeOrigin: true,
      },
    },
  },
});
