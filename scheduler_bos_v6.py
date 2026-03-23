from collections import deque


def compute_D_full(tasks):
    """
    初始化：整張圖算一次 D
    """
    topo = topo_sort(tasks)
    for t in reversed(topo):
        active_succ = [s for s in t.succ if s.status != "done"]
        if not active_succ:
            t.D = t.p
        else:
            t.D = t.p + max(
                s.D - t.delta_to_succ.get(s, 0)
                for s in active_succ
            )


def update_D_from(task):
    """
    🔥 核心：只更新 affected nodes（往上傳）
    """
    queue = deque([task])
    visited = set()

    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)

        active_succ = [s for s in node.succ if s.status != "done"]

        if not active_succ:
            new_D = node.p
        else:
            new_D = node.p + max(
                s.D - node.delta_to_succ.get(s, 0)
                for s in active_succ
            )

        if new_D != node.D:
            node.D = new_D
            for pred in node.pred:
                queue.append(pred)


def topo_sort(tasks):
    indeg = {t: 0 for t in tasks}
    for t in tasks:
        for s in t.succ:
            indeg[s] += 1

    q = deque([t for t in tasks if indeg[t] == 0])
    order = []

    while q:
        t = q.popleft()
        order.append(t)
        for s in t.succ:
            indeg[s] -= 1
            if indeg[s] == 0:
                q.append(s)

    return order


def bos_v6_select(ready_tasks):
    """
    🔥 不再計算 D，只用當前狀態
    """
    ready_tasks.sort(key=lambda x: (-x.D, x.ready_since, x.id))
    return ready_tasks[0]