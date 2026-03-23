from functools import lru_cache


def _build_static_rank(ready_tasks):
    if not ready_tasks:
        return {}

    all_seen = set()

    def visit(task):
        if task in all_seen:
            return
        all_seen.add(task)
        for succ in task.succ:
            visit(succ)

    for task in ready_tasks:
        visit(task)

    @lru_cache(None)
    def rank(task):
        if not task.succ:
            return task.p
        return task.p + max(rank(succ) for succ in task.succ)

    return {task: rank(task) for task in all_seen}


def heft_select(ready_tasks):
    """HEFT-like static downstream rank.
    For this homogeneous-machine MVP, we use an upward-rank style score
    based on the static DAG only, without dynamic residual updates.
    """
    static_rank = _build_static_rank(ready_tasks)
    for task in ready_tasks:
        task.static_rank = static_rank.get(task, task.p)

    ready_tasks.sort(key=lambda x: (-x.static_rank, x.ready_since, x.id))
    return ready_tasks[0]
