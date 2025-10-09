#!/usr/bin/env python3
"""
Consolidate Criterion benchmark results into a single report.

Reads all benchmark results from target/criterion/ and generates:
1. Markdown summary table
2. CSV export for external analysis
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple


def parse_time_ns(estimates: Dict) -> Tuple[float, float, float]:
    """Extract mean, lower, upper from estimates in nanoseconds."""
    mean = estimates.get("mean", {}).get("point_estimate", 0)
    lower = estimates.get("mean", {}).get("confidence_interval", {}).get("lower_bound", 0)
    upper = estimates.get("mean", {}).get("confidence_interval", {}).get("upper_bound", 0)
    return mean, lower, upper


def format_duration(ns: float) -> str:
    """Format nanoseconds into readable units."""
    if ns < 1_000:
        return f"{ns:.2f} ns"
    elif ns < 1_000_000:
        return f"{ns / 1_000:.2f} µs"
    elif ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    else:
        return f"{ns / 1_000_000_000:.2f} s"


def format_throughput(elements: int, time_ns: float) -> str:
    """Calculate and format throughput in elements/sec."""
    if time_ns == 0:
        return "N/A"
    per_sec = elements / (time_ns / 1_000_000_000)
    if per_sec >= 1_000_000:
        return f"{per_sec / 1_000_000:.2f} M/s"
    elif per_sec >= 1_000:
        return f"{per_sec / 1_000:.2f} K/s"
    else:
        return f"{per_sec:.2f} /s"


def collect_benchmarks(criterion_dir: Path) -> List[Dict]:
    """Walk criterion directory and collect all benchmark results."""
    results = []

    for group_dir in criterion_dir.iterdir():
        if not group_dir.is_dir() or group_dir.name == "report":
            continue

        group_name = group_dir.name

        # Check for direct estimates.json (single benchmark in group)
        estimates_path = group_dir / "base" / "estimates.json"
        if estimates_path.exists():
            with open(estimates_path) as f:
                data = json.load(f)
                mean_ns, lower_ns, upper_ns = parse_time_ns(data)
                results.append({
                    "group": group_name,
                    "name": group_name,
                    "mean_ns": mean_ns,
                    "lower_ns": lower_ns,
                    "upper_ns": upper_ns,
                })

        # Check for sub-benchmarks
        for bench_dir in group_dir.iterdir():
            if not bench_dir.is_dir():
                continue

            estimates_path = bench_dir / "base" / "estimates.json"
            if estimates_path.exists():
                with open(estimates_path) as f:
                    data = json.load(f)
                    mean_ns, lower_ns, upper_ns = parse_time_ns(data)
                    results.append({
                        "group": group_name,
                        "name": f"{group_name}/{bench_dir.name}",
                        "mean_ns": mean_ns,
                        "lower_ns": lower_ns,
                        "upper_ns": upper_ns,
                    })

    return sorted(results, key=lambda x: x["name"])


def generate_markdown(results: List[Dict]) -> str:
    """Generate markdown table from results."""
    md = ["# Benchmark Results\n"]
    md.append("| Benchmark | Mean | 95% CI | Throughput* |")
    md.append("|-----------|------|--------|-------------|")

    for r in results:
        mean_str = format_duration(r["mean_ns"])
        ci_str = f"[{format_duration(r['lower_ns'])}, {format_duration(r['upper_ns'])}]"

        # Estimate throughput (assumes 10K elements for most benchmarks)
        # This is a rough estimate - actual varies by benchmark
        elements = 10_000
        if "100000" in r["name"] or "100k" in r["name"]:
            elements = 100_000
        elif "64k" in r["name"] or "65536" in r["name"]:
            elements = 65_536
        elif "8k" in r["name"] or "8192" in r["name"]:
            elements = 8_192

        throughput = format_throughput(elements, r["mean_ns"])

        md.append(f"| {r['name']} | {mean_str} | {ci_str} | {throughput} |")

    md.append("\n*Throughput is estimated based on typical batch sizes")
    return "\n".join(md)


def generate_csv(results: List[Dict]) -> str:
    """Generate CSV export from results."""
    csv = ["benchmark,mean_ns,lower_ns,upper_ns,mean_readable"]
    for r in results:
        csv.append(
            f"{r['name']},{r['mean_ns']},{r['lower_ns']},{r['upper_ns']},"
            f'"{format_duration(r["mean_ns"])}"'
        )
    return "\n".join(csv)


def main():
    criterion_dir = Path("target/criterion")

    if not criterion_dir.exists():
        print("Error: target/criterion/ not found. Run 'cargo bench' first.")
        return 1

    print("Collecting benchmark results...")
    results = collect_benchmarks(criterion_dir)

    if not results:
        print("No benchmark results found.")
        return 1

    print(f"Found {len(results)} benchmarks")

    # Generate outputs
    markdown = generate_markdown(results)
    csv = generate_csv(results)

    # Write files
    output_dir = Path("target/bench-report")
    output_dir.mkdir(exist_ok=True)

    md_path = output_dir / "summary.md"
    csv_path = output_dir / "results.csv"

    md_path.write_text(markdown)
    csv_path.write_text(csv)

    print(f"\nReports generated:")
    print(f"  - {md_path}")
    print(f"  - {csv_path}")
    print(f"\nView summary: cat {md_path}")

    # Print summary to console
    print("\n" + markdown)

    return 0


if __name__ == "__main__":
    exit(main())
