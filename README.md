## Mars Taskflow

Mars Taskflow is a lightweight Python workflow engine for defining tasks and wiring them together as a small DAG (directed acyclic graph). It is designed to be simple enough for coding challenges while still demonstrating real-world patterns such as dependency management, cycle detection, and structured error reporting.

### Features

- **Declarative task definition** using a small Python API.
- **Dependency-aware execution** that runs tasks in the correct order.
- **Cycle detection** to prevent invalid graphs.
- **Rich CLI** that can list tasks, validate the graph, and run a subset of tasks.

### Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

mars-taskflow run example:all
```

### Project layout

- `mars_taskflow/`: library code
- `tests/`: unit tests

