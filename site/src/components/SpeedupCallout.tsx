import { benchmarks, speedupVs } from "../lib/benchmarks";

/** Headline "Weft is N× faster than X" stat cards. Falls back to a pending message pre-run. */
export default function SpeedupCallout() {
  const others = benchmarks.engines.filter((e) => e.key !== "weft");
  const cards = others
    .map((e) => ({ name: e.name, mult: speedupVs(e.key) }))
    .filter((c) => c.mult != null) as { name: string; mult: number }[];

  if (cards.length === 0) {
    return (
      <div className="weft-card p-5 text-sm text-muted">
        Speedups will be published here once the fresh benchmark run completes.
      </div>
    );
  }

  return (
    <div className="grid gap-4 sm:grid-cols-3">
      {cards.map((c) => {
        const faster = c.mult >= 1;
        const pct = Math.abs(c.mult - 1) * 100;
        return (
          <div key={c.name} className="weft-card p-5">
            <div className="text-3xl font-bold tabular-nums text-accent">
              {c.mult.toFixed(2)}×
            </div>
            <div className="mt-1 text-sm text-body">
              {faster ? "faster than" : "vs"} {c.name}
            </div>
            <div className="mt-0.5 text-xs text-muted">
              {faster ? `${pct.toFixed(0)}% lower total runtime` : `${pct.toFixed(0)}% slower`}
            </div>
          </div>
        );
      })}
    </div>
  );
}
