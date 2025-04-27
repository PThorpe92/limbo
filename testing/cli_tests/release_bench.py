#!/usr/bin/env python3

from pathlib import Path
import subprocess
import statistics
import argparse
from time import perf_counter, sleep
from typing import Dict

from cli_tests.test_limbo_cli import TestLimboShell
from cli_tests.console import info, error, test

DEFAULT_LIMBO_BIN = Path("./target/release/limbo")
DB_FILE = Path("testing/temp.db")


def append_time(i, times, start, perf_counter):
    if i != 1:  # don't count first run
        times.append(perf_counter() - start)
    return True


def bench_one(binary: Path, sql: str, iterations: int) -> list[float]:
    """
    Launch a single Limbo process with the given binary, run `sql`
    `iterations` times, and return a list of elapsed wall-clock times.
    """
    shell = TestLimboShell(
        exec_name=str(binary),
        flags=f"-q -t testing/trace.log -m list --vfs syscall {DB_FILE}",
        init_commands="",
    )

    times: list[float] = []

    for i in range(1, iterations + 1):
        start = perf_counter()
        _ = shell.run_test_fn(
            sql, lambda x: x is not None and append_time(i, times, start, perf_counter)
        )
        if i > 1:
            test(f"  {binary.name} | run {i:>3}: {times[-1]:.6f}s")

    shell.quit()
    return times


def setup_temp_db() -> None:
    cleanup_temp_db()
    cmd = ["sqlite3", "testing/testing.db", ".clone testing/temp.db"]
    subprocess.run(cmd, check=True)
    sleep(0.3)  # ensure completion


def cleanup_temp_db() -> None:
    if DB_FILE.exists():
        DB_FILE.unlink()
    wal_file = DB_FILE.with_name(DB_FILE.name + "-wal")
    if wal_file.exists():
        wal_file.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark a SQL statement against two Limbo binaries."
    )
    parser.add_argument("sql", help="SQL statement to execute (quote it)")
    parser.add_argument("iterations", type=int, help="Number of repetitions")
    parser.add_argument(
        "other_binary",
        type=str,
        help="Path to second Limbo binary to compare (first is ./target/release/limbo)",
    )
    args = parser.parse_args()
    setup_temp_db()

    sql, iterations = args.sql, args.iterations
    if iterations <= 0:
        error("Iterations must be a positive integer")
        parser.error("Invalid Arguments")

    binaries = [DEFAULT_LIMBO_BIN, Path(args.other_binary)]

    info(f"SQL        : {sql}")
    info(f"Iterations : {iterations}")
    info(f"Database   : {DB_FILE.resolve()}")
    info("-" * 60)

    averages: Dict[str, float] = {}

    for i, binary in enumerate(binaries):
        test(f"\n### Binary: {binary.name} ###")
        times = bench_one(binary, sql, iterations)
        info(f"All times ({binary.name}):", " ".join(f"{t:.6f}" for t in times))
        avg = statistics.mean(times)
        if i == 0:
            averages[binary.name] = avg
        else:
            averages["other"] = avg

    info("\n" + "-" * 60)
    info("Average runtime per binary")
    info("-" * 60)

    baseline = DEFAULT_LIMBO_BIN.name
    baseline_avg = averages[baseline]

    name_pad = max(len(name) for name in averages)

    for name, avg in averages.items():
        if name == baseline:
            print(f"{name} : {avg:.6f}  (baseline)")
            info(f"{name:<{name_pad}} : {avg:.6f}  (baseline)")
        elif name == "other":
            pct = (avg - baseline_avg) / baseline_avg * 100.0
            faster_slower = "slower" if pct > 0 else "faster"
            print(
                f"{name} : {avg:.6f}  ({abs(pct):.1f}% {faster_slower} than {baseline})"
            )
            info(
                f"{name:<{name_pad}} : {avg:.6f}  ({abs(pct):.1f}% {faster_slower} than {baseline})"
            )
    info("-" * 60)

    cleanup_temp_db()


if __name__ == "__main__":
    main()
