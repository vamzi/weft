/* A small muted banner flagging sections that still render mock fixtures. */

export function DemoNote({ text }: { text?: string }) {
  return (
    <div className="mb-4 flex items-center gap-2 rounded-weft-sm border border-hairline bg-bg-subtle px-3 py-2 text-xs text-muted">
      <span
        className="inline-block h-1.5 w-1.5 shrink-0 rounded-full"
        style={{ backgroundColor: "var(--weft-warning)" }}
      />
      {text ?? "Demo data — live wiring pending."}
    </div>
  );
}
