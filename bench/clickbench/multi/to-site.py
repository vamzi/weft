#!/usr/bin/env python3
"""Merge results/<engine>.json into site/src/data/benchmarks.json (the chart data source).

Per query, the *hot* time is min(try2, try3) (the ClickBench convention). An engine with no
results/<engine>.json yet stays `pending` with a null total, so the site renders a muted
"awaiting run" bar and the layout is final before any number exists. Failed queries are kept as
`null` in perQuery (shown as a gap), never dropped.

  python3 bench/clickbench/multi/to-site.py [--run-date YYYY-MM-DD] [--machine c6a.4xlarge]
"""
import argparse
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
RESULTS = os.path.join(HERE, "results")
OUT = os.path.join(REPO, "site", "src", "data", "benchmarks.json")

# Fixed display order + labels. `key` matches results/<key>.json and run-engine.sh.
ENGINES = [
    {"key": "weft", "name": "Weft", "highlight": True},
    {"key": "sail", "name": "Sail", "highlight": False},
    {"key": "spark", "name": "Spark", "highlight": False},
    {"key": "gluten", "name": "Spark + Gluten/Velox", "highlight": False},
]


def hot(tries):
    """ClickBench hot = min of the 2nd/3rd try, ignoring nulls; None if the query never ran."""
    vals = [t for t in tries[1:] if t is not None]
    return round(min(vals), 4) if vals else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", default=None)
    ap.add_argument("--machine", default="c6a.4xlarge")
    args = ap.parse_args()

    out_engines = []
    run_date = args.run_date
    for spec in ENGINES:
        path = os.path.join(RESULTS, f"{spec['key']}.json")
        if not os.path.exists(path):
            out_engines.append({
                "key": spec["key"], "name": spec["name"], "highlight": spec["highlight"],
                "total": None, "source": "pending", "perQuery": [], "failures": None,
            })
            continue
        data = json.load(open(path))
        per_query = [hot(row) for row in data.get("result", [])]
        total = round(sum(h for h in per_query if h is not None), 3) if per_query else None
        failures = sum(1 for h in per_query if h is None)
        run_date = run_date or data.get("date")
        out_engines.append({
            "key": spec["key"], "name": spec["name"], "highlight": spec["highlight"],
            "total": total,
            "source": f"measured (EC2 {args.machine} {run_date})",
            "perQuery": per_query,
            "failures": failures,
        })

    doc = {
        "dataset": "hits.parquet — 99,997,497 rows, 14.78 GB (ClickBench)",
        "machine": args.machine,
        "runDate": run_date,
        "queryCount": 43,
        "method": "Spark Connect, stock PySpark client, 3 tries/query, hot = min(try2, try3)",
        "engines": out_engines,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    measured = [e["name"] for e in out_engines if e["total"] is not None]
    print(f"wrote {OUT}\n  measured: {measured or 'none yet'}")


if __name__ == "__main__":
    main()
