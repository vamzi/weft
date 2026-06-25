import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Project page at https://vamzi.github.io/weft/ → assets must resolve under /weft/.
// Override with VITE_BASE='/' for a custom domain / user page.
const base = process.env.VITE_BASE ?? "/weft/";

export default defineConfig({
  base,
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  server: { port: 5174 },
});
