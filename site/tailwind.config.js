/** @type {import('tailwindcss').Config} */
// Mirrors web/tailwind.config.js so the showcase site and the control plane share one theme.
export default {
  darkMode: ['selector', ':root[data-theme="dark"]'],
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--weft-bg)",
        "bg-subtle": "var(--weft-bg-subtle)",
        surface: "var(--weft-surface)",
        hairline: "var(--weft-border)",
        body: "var(--weft-text)",
        muted: "var(--weft-text-muted)",
        accent: "var(--weft-accent)",
        "accent-hover": "var(--weft-accent-hover)",
        "accent-contrast": "var(--weft-accent-contrast)",
        success: "var(--weft-success)",
        warning: "var(--weft-warning)",
        danger: "var(--weft-danger)",
        "code-bg": "var(--weft-code-bg)",
        "code-text": "var(--weft-code-text)",
      },
      fontFamily: {
        sans: "var(--weft-font-ui)",
        mono: "var(--weft-font-mono)",
      },
      borderRadius: {
        weft: "var(--weft-radius)",
        "weft-sm": "var(--weft-radius-sm)",
      },
      maxWidth: { content: "1100px" },
    },
  },
  plugins: [],
};
