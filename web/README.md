# Weft web — control-plane SPA

The browser-facing workspace (clusters, catalog, permissions, SQL editor, notebooks, dashboards,
jobs). Talks **only** to the gateway REST/WebSocket API (`crates/weft-gateway` → `ROUTES`); never
gRPC, never the clusters directly.

## Stack (frozen contract)

- **React + TypeScript + Vite**
- **Tailwind + shadcn/ui**, themed entirely from `src/styles/theme.css` design tokens
  (ollama.com-inspired: light primary, dark code surfaces, warm-orange accent)
- **Monaco** for the SQL/notebook editors (Spark-SQL/PySpark Monarch grammar, governed-catalog
  `CompletionItemProvider`, hover/signature help)
- **TanStack Query** for gateway calls; **apache-arrow** (JS) to decode streamed Arrow IPC results
  into a virtualized grid
- **Vega-Lite / ECharts** for dashboards (client-rendered)

## Layout (Wave 1 shell, then one feature area per Wave-2 agent)

```
src/
  styles/theme.css      # design tokens (this is the theme contract)
  app/                  # router, auth flow, layout (navbar + left icon rail)
  lib/api.ts            # typed client generated from the gateway OpenAPI
  clusters/  catalog/  permissions/  sql-editor/  notebooks/  dashboards/  jobs/  ai/
```

Bootstrap (`package.json`, Vite config, Tailwind config) lands with the Wave-1 web-shell agent;
the theme tokens above are committed first so every feature area inherits them.

## Build & run

Requires node 24 + npm 11.

```bash
npm install        # install deps
npm run dev        # Vite dev server on http://localhost:5173
npm run build      # type-check (tsc --noEmit) + Vite production build → dist/
npm run preview    # serve the built dist/
npm run typecheck  # tsc --noEmit only
```

The dev server proxies `/api`, `/scim`, and `/healthz` to a gateway at
`http://localhost:8080` (see `vite.config.ts`).

## Current shell (Wave 1)

- **Stack:** Vite + React + TypeScript + Tailwind, themed entirely from
  `src/styles/theme.css` via `tailwind.config.js` (Tailwind color names map onto
  the `--weft-*` CSS variables — never hard-code a color).
- **Shell:** sticky top navbar (logo + theme toggle + sign-in/workspace) and a
  left icon rail for the workspace sections, wired with React Router
  (`src/App.tsx`). Light mode is primary; the dark toggle sets
  `data-theme="dark"` on `:root` and is persisted to `localStorage`
  (`weft-theme`), applied pre-paint in `index.html` to avoid a flash.
- **API client:** `src/lib/api.ts` is a typed client mirroring the gateway
  `ROUTES`. It returns mocked data today; flip `USE_MOCK = false` to hit the live
  gateway (each function already routes through `request()` at the real path).
- **Clusters** (`/clusters`) is fully working against the mock client: list,
  create (name / size / min-max workers), and per-cluster Start / Stop / Delete
  with semantic status colors.
- **Catalog / SQL / Notebooks / Dashboards / Jobs** are titled stubs so routing
  is complete; each notes the gateway routes it will use.
