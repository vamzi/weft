import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  server: {
    port: 5173,
    // The gateway REST/WS edge (crates/weft-gateway). Mocked today; flip the
    // client's USE_MOCK flag to talk to a live gateway here.
    proxy: {
      "/api": "http://localhost:8080",
      "/scim": "http://localhost:8080",
      "/healthz": "http://localhost:8080",
    },
  },
});
