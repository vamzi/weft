import { Link } from "react-router-dom";
import CodeBlock from "../components/CodeBlock";
import BenchmarkChart from "../components/BenchmarkChart";
import { benchmarks, speedupVs } from "../lib/benchmarks";

const REPO = "https://github.com/vamzi/weft";

const FEATURES = [
  {
    title: "Drop-in Spark Connect",
    body: "Speaks the Spark Connect protocol. Point unmodified PySpark or Spark SQL at sc:// — change one line, keep your code.",
  },
  {
    title: "No JVM, written in Rust",
    body: "A lean vectorized core (Loom, on Arrow + DataFusion) executes plans natively. No JVM to tune, no GC pauses, fast cold starts.",
  },
  {
    title: "Faster on ClickBench",
    body: "Weft's north-star benchmark is ClickBench on the real 14.78 GB hits dataset — measured head-to-head against Sail, Spark, and Spark+Gluten.",
  },
  {
    title: "Open formats, governed",
    body: "Reads Parquet, Delta, and Iceberg through a pluggable catalog (Hive, Unity, Glue). Your lakehouse, your tables.",
  },
];

function Hero() {
  const vsSpark = speedupVs("spark");
  return (
    <section className="border-b border-hairline">
      <div className="weft-container py-16 sm:py-24">
        <div className="grid items-center gap-10 lg:grid-cols-2">
          <div>
            <span className="weft-eyebrow">Drop-in Apache Spark replacement</span>
            <h1 className="mt-3 text-4xl font-bold leading-tight tracking-tight sm:text-5xl">
              Spark speed,{" "}
              <span className="text-accent">without the JVM.</span>
            </h1>
            <p className="mt-5 max-w-xl text-lg text-muted">
              Weft runs your existing PySpark and Spark SQL through a vectorized Rust engine.
              Same Spark Connect API, none of the JVM — and it's{" "}
              {vsSpark ? `${(vsSpark).toFixed(1)}× faster than Spark` : "faster"} on ClickBench.
            </p>
            <div className="mt-7 flex flex-wrap gap-3">
              <a href={REPO} className="weft-btn-primary">
                Get started on GitHub
              </a>
              <Link to="/performance" className="weft-btn-ghost">
                See the benchmarks →
              </Link>
            </div>
          </div>
          <div>
            <CodeBlock
              lines={[
                { text: "# 1. start the Weft Spark Connect server", comment: true },
                { text: "weft spark server --port 50051" },
                { text: "" },
                { text: "# 2. point a stock PySpark client at it", comment: true },
                { text: "from pyspark.sql import SparkSession" },
                { text: 'spark = (SparkSession.builder' },
                { text: '         .remote("sc://localhost:50051")' },
                { text: "         .getOrCreate())" },
                { text: 'spark.sql("SELECT count(*) FROM hits").show()' },
              ]}
              copy={'weft spark server --port 50051'}
            />
            <p className="mt-2 text-center text-xs text-muted">
              Your Spark code is unchanged — only the <code className="font-mono">sc://</code> URL.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}

export default function HomePage() {
  return (
    <>
      <Hero />

      {/* Feature grid */}
      <section className="weft-container py-16">
        <div className="grid gap-px overflow-hidden rounded-weft border border-hairline bg-hairline sm:grid-cols-2">
          {FEATURES.map((f) => (
            <div key={f.title} className="bg-bg p-6">
              <h3 className="text-base font-semibold">{f.title}</h3>
              <p className="mt-2 text-sm leading-relaxed text-muted">{f.body}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Benchmark teaser */}
      <section className="border-t border-hairline bg-bg-subtle">
        <div className="weft-container py-16">
          <div className="mx-auto mb-8 max-w-2xl text-center">
            <span className="weft-eyebrow">Performance</span>
            <h2 className="mt-2 text-2xl font-bold tracking-tight sm:text-3xl">
              Measured, not marketed.
            </h2>
            <p className="mt-3 text-muted">
              All four engines run the same 43 ClickBench queries on the same {benchmarks.machine},
              over the same {benchmarks.dataset}. Same Spark SQL, same client — only the engine
              changes.
            </p>
          </div>
          <div className="mx-auto max-w-3xl">
            <BenchmarkChart engines={benchmarks.engines} />
            <div className="mt-6 text-center">
              <Link to="/performance" className="weft-btn-ghost">
                Full methodology & per-query results →
              </Link>
            </div>
          </div>
        </div>
      </section>
    </>
  );
}
