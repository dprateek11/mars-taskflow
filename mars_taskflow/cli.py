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
            description="Provides the base integer.",
        )
    )

    graph.add_task(
        Task(
            name="exponent",
            dependencies=[],
            func=lambda ctx: cfg.exponent,
            description="Provides the exponent integer.",
        )
    )

    def power(ctx: Dict[str, int]) -> int:
        return ctx["base"] ** ctx["exponent"]

    graph.add_task(
        Task(
            name="power",
            dependencies=["base", "exponent"],
            func=power,
            description="Raises base to the exponent.",
        )
    )

    graph.add_task(
        Task(
            name="description",
            dependencies=["power"],
            func=lambda ctx: f"{cfg.base}^{cfg.exponent} = {ctx['power']}",
            description="Human-readable description of the computation.",
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

    validate = sub.add_parser("validate", help="Validate the example graph")

    plan = sub.add_parser("plan", help="Show execution plan for a target")
    plan.add_argument("target", nargs="?", default="description", help="Target task to plan for")

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
        task = graph.get(name)
        suffix = f" - {task.description}" if task.description else ""
        print(f"{name}{suffix}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    graph = build_example_graph(ExampleConfig())
    problems = graph.validate()
    if not problems:
        print("Graph is valid.")
        return 0
    print("Graph has problems:")
    for msg in problems:
        print(f"- {msg}")
    return 1


def cmd_plan(args: argparse.Namespace) -> int:
    graph = build_example_graph(ExampleConfig())
    levels = graph.plan(targets=[args.target])
    if not levels:
        print("Nothing to run.")
        return 1
    for level, names in levels:
        joined = ", ".join(names)
        print(f"Level {level}: {joined}")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "list":
        return cmd_list(args)
    if args.command == "validate":
        return cmd_validate(args)
    if args.command == "plan":
        return cmd_plan(args)
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

