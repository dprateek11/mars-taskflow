from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Dict, Iterable, List

from .engine import Task, TaskGraph


@dataclass
class ExampleConfig:
    base: int = 2
    exponent: int = 8


def build_example_graph(cfg: ExampleConfig) -> TaskGraph:
    graph = TaskGraph()

    graph.add_task(
        Task(
            name="base",
            dependencies=[],
            func=lambda ctx: cfg.base,
        )
    )

    graph.add_task(
        Task(
            name="exponent",
            dependencies=[],
            func=lambda ctx: cfg.exponent,
        )
    )

    def power(ctx: Dict[str, int]) -> int:
        return ctx["base"] ** ctx["exponent"]

    graph.add_task(Task(name="power", dependencies=["base", "exponent"], func=power))

    graph.add_task(
        Task(
            name="description",
            dependencies=["power"],
            func=lambda ctx: f"{cfg.base}^{cfg.exponent} = {ctx['power']}",
        )
    )

    return graph


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mars Taskflow - tiny DAG task runner")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run the example workflow")
    run.add_argument("target", nargs="?", default="description", help="Name of the target task to run")
    run.add_argument("--base", type=int, default=2, help="Base integer")
    run.add_argument("--exponent", type=int, default=8, help="Exponent integer")

    sub.add_parser("list", help="List available tasks in the example graph")

    return parser.parse_args(list(argv) if argv is not None else None)


def cmd_run(args: argparse.Namespace) -> int:
    cfg = ExampleConfig(base=args.base, exponent=args.exponent)
    graph = build_example_graph(cfg)
    results = graph.execute(targets=[args.target])
    print(results[args.target])
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    graph = build_example_graph(ExampleConfig())
    names: List[str] = sorted(task.name for task in graph.tasks())
    for name in names:
        print(name)
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "list":
        return cmd_list(args)
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

