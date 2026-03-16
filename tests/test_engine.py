from mars_taskflow.engine import Task, TaskGraph


def test_simple_linear_execution():
    g = TaskGraph()
    g.add_task(Task("a", lambda ctx: 1))
    g.add_task(Task("b", lambda ctx: ctx["a"] + 1, dependencies=["a"]))

    results = g.execute()
    assert results == {"a": 1, "b": 2}


def test_cycle_detection_raises():
    g = TaskGraph()
    g.add_task(Task("a", lambda ctx: 1, dependencies=["b"]))
    g.add_task(Task("b", lambda ctx: 2, dependencies=["a"]))

    try:
        g.execute()
    except ValueError as exc:
        assert "cycle" in str(exc).lower()
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected ValueError for cycle")

