/* Semantic status pill driven by theme.css success/warning/danger tokens. */

type Tone = "success" | "warning" | "danger" | "muted";

const toneStyle: Record<Tone, { color: string; bg: string }> = {
  success: { color: "var(--weft-success)", bg: "color-mix(in srgb, var(--weft-success) 14%, transparent)" },
  warning: { color: "var(--weft-warning)", bg: "color-mix(in srgb, var(--weft-warning) 14%, transparent)" },
  danger: { color: "var(--weft-danger)", bg: "color-mix(in srgb, var(--weft-danger) 14%, transparent)" },
  muted: { color: "var(--weft-text-muted)", bg: "color-mix(in srgb, var(--weft-text-muted) 14%, transparent)" },
};

export function StatusBadge({ tone, label }: { tone: Tone; label: string }) {
  const s = toneStyle[tone];
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium capitalize"
      style={{ color: s.color, backgroundColor: s.bg }}
    >
      <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: s.color }} />
      {label}
    </span>
  );
}
