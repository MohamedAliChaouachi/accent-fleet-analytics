"""
run_all_figures.py
===================
Run every ``fig_*.py`` script under scripts/python/figures/ and write the
corresponding PDF under report/figures/.

Each script is executed as a subprocess so a failure in one figure does
not abort the others; a final summary is printed and the exit code is
non-zero if any figure failed.

Usage
-----
    python scripts/python/run_all_figures.py
    python scripts/python/run_all_figures.py --only confusion_matrix risk_score
    python scripts/python/run_all_figures.py --skip distributions
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIGURES_DIR = HERE / "figures"


def discover_scripts() -> list[Path]:
    return sorted(FIGURES_DIR.glob("fig_*.py"))


def run_one(script: Path) -> tuple[Path, int, float]:
    start = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, str(script)],
        cwd=script.parent,
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - start
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    return script, proc.returncode, elapsed


def main() -> int:
    parser = argparse.ArgumentParser(description="Run all figure scripts.")
    parser.add_argument("--only", nargs="*", default=None,
                        help="Substrings; only matching scripts run.")
    parser.add_argument("--skip", nargs="*", default=[],
                        help="Substrings; matching scripts are skipped.")
    args = parser.parse_args()

    scripts = discover_scripts()
    if args.only:
        scripts = [s for s in scripts if any(k in s.stem for k in args.only)]
    if args.skip:
        scripts = [s for s in scripts if not any(k in s.stem for k in args.skip)]

    if not scripts:
        print("no figure scripts matched", file=sys.stderr)
        return 1

    print(f"running {len(scripts)} figure script(s)\n" + "-" * 60)
    results = []
    for s in scripts:
        print(f"\n>>> {s.name}")
        results.append(run_one(s))

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    failed = 0
    for script, rc, elapsed in results:
        flag = "OK " if rc == 0 else "FAIL"
        if rc != 0:
            failed += 1
        print(f"  [{flag}] {script.name:40s} {elapsed:6.2f} s")
    print("-" * 60)
    print(f"  {len(results) - failed} succeeded, {failed} failed")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
