/*
 * Monaco wiring for the SQL editor.
 *
 * - Registers two themes ("weft-light" / "weft-dark") whose surfaces come from
 *   the --weft-* design tokens, so the editor matches the brand. The dark code
 *   surface (--weft-code-bg) is the default editor background.
 * - Registers a governed-catalog completion provider for the built-in `sql`
 *   language: catalog/schema/table/column objects from the mock catalog API
 *   plus a curated set of SQL keywords. Live, the same provider would be fed by
 *   POST /api/complete instead of the in-memory catalog snapshot.
 */
import type { Monaco } from "@monaco-editor/react";
import type { languages } from "monaco-editor";
import { api, type CatalogObject } from "./api";

export const WEFT_DARK = "weft-dark";
export const WEFT_LIGHT = "weft-light";

/** Resolve a --weft-* token to a concrete hex (Monaco needs literal colors). */
function token(name: string, fallback: string): string {
  if (typeof document === "undefined") return fallback;
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
  "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL OUTER JOIN", "ON", "AS",
  "AND", "OR", "NOT", "IN", "IS NULL", "IS NOT NULL", "BETWEEN", "LIKE", "CASE",
  "WHEN", "THEN", "ELSE", "END", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",
  "WITH", "UNION", "UNION ALL", "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE",
  "CREATE TABLE", "CREATE VIEW", "DROP TABLE", "CAST", "COALESCE", "DATE_TRUNC",
];

let installed = false;
let catalogSnapshot: CatalogObject[] = [];

/**
 * One-time setup: themes + completion provider. Safe to call on every editor
 * mount (guarded by `installed`).
 */
export function setupMonacoSql(monaco: Monaco): void {
  defineThemes(monaco);
  if (installed) return;
  installed = true;

  // Warm the catalog snapshot used for completions.
  api.listCatalog().then((objs) => (catalogSnapshot = objs)).catch(() => {});

  monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [" ", ".", "("],
    provideCompletionItems(model, position) {
      const word = model.getWordUntilPosition(position);
      const range: languages.CompletionItem["range"] = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      const Kind = monaco.languages.CompletionItemKind;
      const suggestions: languages.CompletionItem[] = [];

      // Catalog objects (catalog → schema → table/view → columns).
      for (const obj of catalogSnapshot) {
        const detail =
          obj.kind === "table" || obj.kind === "view"
            ? obj.parent
              ? `${obj.parent}.${obj.name}`
              : obj.name
            : obj.kind;
        suggestions.push({
          label: obj.name,
          kind:
            obj.kind === "catalog"
              ? Kind.Module
              : obj.kind === "schema"
                ? Kind.Class
                : obj.kind === "view"
                  ? Kind.Interface
                  : Kind.Struct,
          insertText: obj.name,
          detail,
          range,
          sortText: `0_${obj.kind}_${obj.name}`,
        });
      }

      // Columns from known tables.
      for (const [tbl, cols] of Object.entries(KNOWN_COLUMNS)) {
        for (const col of cols) {
          suggestions.push({
            label: col,
            kind: Kind.Field,
            insertText: col,
            detail: `column · ${tbl}`,
            range,
            sortText: `1_col_${col}`,
          });
        }
      }

      // SQL keywords.
      for (const kw of SQL_KEYWORDS) {
        suggestions.push({
          label: kw,
          kind: Kind.Keyword,
          insertText: kw,
          range,
          sortText: `2_kw_${kw}`,
        });
      }

      return { suggestions };
    },
  });
}

/** Column hints surfaced in completion; mirrors the mock catalog tables. */
const KNOWN_COLUMNS: Record<string, string[]> = {
  orders: ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority"],
  lineitem: ["l_orderkey", "l_partkey", "l_quantity", "l_extendedprice", "l_discount", "l_returnflag", "l_shipdate"],
  monthly_revenue: ["month", "revenue", "orders"],
};

function defineThemes(monaco: Monaco): void {
  const accent = stripHash(token("--weft-accent", "#ff6a00"));
  const muted = stripHash(token("--weft-text-muted", "#6b7280"));

  monaco.editor.defineTheme(WEFT_DARK, {
    base: "vs-dark",
    inherit: true,
    rules: [
      { token: "keyword.sql", foreground: accent, fontStyle: "bold" },
      { token: "operator.sql", foreground: accent },
      { token: "string.sql", foreground: "9ece6a" },
      { token: "number.sql", foreground: "ff9e64" },
      { token: "comment", foreground: muted, fontStyle: "italic" },
    ],
    colors: {
      "editor.background": token("--weft-code-bg", "#0b0b0c"),
      "editor.foreground": token("--weft-code-text", "#e5e7eb"),
      "editorLineNumber.foreground": "#4b5563",
      "editorCursor.foreground": token("--weft-accent", "#ff6a00"),
      "editor.selectionBackground": "#264f78",
      "editor.lineHighlightBackground": "#15151a",
    },
  });

  monaco.editor.defineTheme(WEFT_LIGHT, {
    base: "vs",
    inherit: true,
    rules: [
      { token: "keyword.sql", foreground: stripHash(token("--weft-accent-hover", "#e85f00")), fontStyle: "bold" },
      { token: "comment", foreground: muted, fontStyle: "italic" },
    ],
    colors: {
      "editor.background": token("--weft-bg", "#ffffff"),
      "editor.foreground": token("--weft-text", "#1a1a1a"),
      "editorLineNumber.foreground": "#9ca3af",
      "editorCursor.foreground": token("--weft-accent", "#ff6a00"),
    },
  });
}

function stripHash(c: string): string {
  return c.startsWith("#") ? c.slice(1) : c;
}
