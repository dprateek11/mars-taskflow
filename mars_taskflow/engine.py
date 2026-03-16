from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Set


class TaskExecutionError(RuntimeError):
    def __init__(self, task_name: str, original: BaseException):
        super().__init__(f"Task '{task_name}' failed: {original}")
        self.task_name = task_name
        self.original = original


@dataclass
class Task:
    name: str
    func: Callable[[Mapping[str, Any]], Any]
    dependencies: List[str] = field(default_factory=list)

    def run(self, context: Mapping[str, Any]) -> Any:
        return self.func(context)


class TaskGraph:
    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}

    def add_task(self, task: Task) -> None:
        if task.name in self._tasks:
            raise ValueError(f"Task '{task.name}' already exists")
        for dep in task.dependencies:
            if dep == task.name:
                raise ValueError("Task cannot depend on itself")
        self._tasks[task.name] = task

    def tasks(self) -> Iterable[Task]:
        return self._tasks.values()

    def get(self, name: str) -> Task:
        return self._tasks[name]

    def _build_adjacency(self) -> tuple[Dict[str, Set[str]], Dict[str, int]]:
        adjacency: Dict[str, Set[str]] = defaultdict(set)
        indegree: Dict[str, int] = defaultdict(int)
        for task in self._tasks.values():
            indegree.setdefault(task.name, 0)
            for dep in task.dependencies:
                adjacency[dep].add(task.name)
                indegree[task.name] += 1
        return adjacency, indegree

    def topological_order(self) -> List[str]:
        adjacency, indegree = self._build_adjacency()
        queue: deque[str] = deque(name for name, deg in indegree.items() if deg == 0)
        order: List[str] = []

        while queue:
            node = queue.popleft()
            order.append(node)
            for neighbor in adjacency.get(node, ()):
                indegree[neighbor] -= 1
                if indegree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(self._tasks):
            raise ValueError("Task graph contains a cycle")

        return order

    def execute(self, targets: Optional[Iterable[str]] = None, initial_context: Optional[MutableMapping[str, Any]] = None) -> Dict[str, Any]:
        if not self._tasks:
            return {}

        order = self.topological_order()
        if targets is not None:
            target_set = set(targets)
            unknown = target_set - set(self._tasks.keys())
            if unknown:
                raise KeyError(f"Unknown target tasks: {', '.join(sorted(unknown))}")
        else:
            target_set = set(self._tasks.keys())

        context: Dict[str, Any] = dict(initial_context or {})
        results: Dict[str, Any] = {}

        for name in order:
            if name not in target_set and not any(t in target_set for t in self._downstream(name)):
                continue

            task = self._tasks[name]
            try:
                result = task.run(context)
            except BaseException as exc:  # pragma: no cover - defensive
                raise TaskExecutionError(name, exc) from exc

            results[name] = result
            context[name] = result

        return results

    def _downstream(self, name: str) -> Set[str]:
        adjacency, _ = self._build_adjacency()
        seen: Set[str] = set()
        queue: deque[str] = deque([name])

        while queue:
            node = queue.popleft()
            for neighbor in adjacency.get(node, ()):
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append(neighbor)

        return seen
