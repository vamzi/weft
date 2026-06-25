/*
 * Monaco wiring for the SQL editor.
 *
 * - Registers two themes ("weft-light" / "weft-dark") whose surfaces come from
 *   the --weft-* design tokens, so the editor matches the brand. The dark code
 *   surface (--weft-code-bg) is the default editor background.
 * - Registers a governed-catalog completion provider for the built-in `sql`
 *   language: catalog/schema/table/column objects from the LIVE catalog API
 *   (GET /api/catalog) plus a curated set of SQL keywords.
 */
import type { Monaco } from "@monaco-editor/react";
import type { languages } from "monaco-editor";
import { api, type CatalogNamespace } from "./api";

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
let catalogSnapshot: CatalogNamespace[] = [];

/**
 * One-time setup: themes + completion provider. Safe to call on every editor
 * mount (guarded by `installed`).
 */
export function setupMonacoSql(monaco: Monaco): void {
  defineThemes(monaco);
  if (installed) return;
  installed = true;

  // Warm the snapshot of the LIVE catalog used for completions.
  api.getCatalog().then((cat) => (catalogSnapshot = cat)).catch(() => {});

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
      const seenColumns = new Set<string>();

      // Live catalog (catalog → schema → table → columns).
      for (const cat of catalogSnapshot) {
        suggestions.push({
          label: cat.name,
          kind: Kind.Module,
          insertText: cat.name,
          detail: "catalog",
          range,
          sortText: `0_catalog_${cat.name}`,
        });
        for (const schema of cat.schemas) {
          const schemaFqn = `${cat.name}.${schema.name}`;
          suggestions.push({
            label: schema.name,
            kind: Kind.Class,
            insertText: schema.name,
            detail: `schema · ${schemaFqn}`,
            range,
            sortText: `0_schema_${schema.name}`,
          });
          for (const table of schema.tables) {
            const tableFqn = `${schemaFqn}.${table.name}`;
            suggestions.push({
              label: table.name,
              kind: Kind.Struct,
              insertText: table.name,
              detail: `table · ${tableFqn}`,
              range,
              sortText: `0_table_${table.name}`,
            });
            for (const col of table.columns) {
              // De-dupe identically-named columns across tables.
              if (seenColumns.has(col.name)) continue;
              seenColumns.add(col.name);
              suggestions.push({
                label: col.name,
                kind: Kind.Field,
                insertText: col.name,
                detail: `${col.data_type} · ${table.name}`,
                range,
                sortText: `1_col_${col.name}`,
              });
            }
          }
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
