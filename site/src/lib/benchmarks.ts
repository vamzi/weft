import raw from "../data/benchmarks.json";

export interface Engine {
  key: string;
  name: string;
  highlight: boolean;
  /** Total over the common set (queries ALL engines completed); the fair headline number. */
  total: number | null;
  /** This engine's full total over every query it completed. */
  totalAll?: number | null;
  /** "measured (EC2 c6a.4xlarge <date>)" or "pending". */
  source: string;
  /** Per-query hot seconds (min of try2/try3); entries null for failed queries. */
  perQuery: (number | null)[];
  failures: number | null;
  /** Indices of queries this engine could not execute. */
  failedQueries?: number[];
}

export interface Benchmarks {
  dataset: string;
  machine: string;
  runDate: string | null;
  queryCount: number;
  /** Number of queries every measured engine completed (basis for the fair total). */
  commonCount?: number;
  method: string;
  engines: Engine[];
}

export const benchmarks = raw as Benchmarks;

export function isMeasured(e: Engine): boolean {
  return e.total != null;
}

export const measuredEngines = benchmarks.engines.filter(isMeasured);

/** Weft's speedup vs another engine as a multiple (e.g. 1.24 = 24% faster), or null. */
export function speedupVs(otherKey: string): number | null {
  const weft = benchmarks.engines.find((e) => e.key === "weft");
  const other = benchmarks.engines.find((e) => e.key === otherKey);
  if (!weft?.total || !other?.total) return null;
  return other.total / weft.total;
}
