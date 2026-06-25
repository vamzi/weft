import { useEffect, useState } from "react";
import { benchmarks, speedupVs } from "../lib/benchmarks";
import { useInView, prefersReducedMotion } from "../lib/useInView";

/** "Weft is N× faster than X" chips that count up from 1.00× once in view. Honest: only renders
 *  engines with a measured total; falls back to a pending note. */
export default function StatChips() {
  const cards = benchmarks.engines
    .filter((e) => e.key !== "weft")
    .map((e) => ({ name: e.name, mult: speedupVs(e.key) }))
    .filter((c): c is { name: string; mult: number } => c.mult != null);

  const { ref, inView } = useInView<HTMLDivElement>();

  if (cards.length === 0) {
    return (
      <p className="text-sm text-muted">
        Speedups publish here once the fresh benchmark run completes.
      </p>
    );
  }

  return (
    <div ref={ref} className="flex flex-wrap gap-3">
      {cards.map((c) => (
        <Chip key={c.name} name={c.name} mult={c.mult} go={inView} />
      ))}
    </div>
  );
}

function Chip({ name, mult, go }: { name: string; mult: number; go: boolean }) {
  const [val, setVal] = useState(prefersReducedMotion() ? mult : 1);
  useEffect(() => {
    if (!go || prefersReducedMotion()) {
      setVal(mult);
      return;
    }
    const start = performance.now();
    const dur = 900;
    let raf = 0;
    const tick = (t: number) => {
      const p = Math.min(1, (t - start) / dur);
      const eased = 1 - Math.pow(1 - p, 3);
      setVal(1 + (mult - 1) * eased);
      if (p < 1) raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [go, mult]);

  return (
    <div className="rounded-weft border border-hairline bg-surface px-4 py-2.5">
      <span className="text-xl font-bold tabular-nums text-accent">{val.toFixed(2)}×</span>
      <span className="ml-2 text-sm text-muted">faster than {name}</span>
    </div>
  );
}
