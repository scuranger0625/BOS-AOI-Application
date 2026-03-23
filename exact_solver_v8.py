from itertools import combinations
from math import ceil
from copy import deepcopy


class ExactOptimalSolver:
    """
    小型 instance 用的 branch-and-bound。
    假設目標為 makespan，且可限制在小任務數（建議 <= 8）。
    這不是拿去跑大規模工廠的，別鬧它。
    """

    def __init__(self, jobs, machine_count=2):
        self.jobs = jobs
        self.machine_count = machine_count
        self.tasks = [task for job in jobs for task in job.tasks]
        self.index = {task.id: idx for idx, task in enumerate(self.tasks)}
        self.n = len(self.tasks)
        self.best = float("inf")
        self.best_schedule = None
        self.total_work = sum(t.p for t in self.tasks)

        self.pred_idx = [[] for _ in self.tasks]
        self.succ_delta = [{} for _ in self.tasks]
        self.release = [t.job_release_time for t in self.tasks]
        self.proc = [t.p for t in self.tasks]
        for t in self.tasks:
            i = self.index[t.id]
            self.pred_idx[i] = [self.index[p.id] for p in t.pred]
            self.succ_delta[i] = {self.index[s.id]: t.delta_to_succ.get(s, 0) for s in t.succ}

    def _earliest_ready_time(self, idx, start_times):
        t = self.release[idx]
        for pred in self.pred_idx[idx]:
            pred_start = start_times[pred]
            if pred_start is None:
                return None
            delta = self.succ_delta[pred].get(idx, 0)
            t = max(t, pred_start + self.proc[pred] - delta)
        return t

    def solve(self):
        state = {
            "time": 0,
            "status": ["waiting"] * self.n,
            "start_times": [None] * self.n,
            "finish_times": [None] * self.n,
            "running": {},  # machine -> task index
            "done_count": 0,
        }
        self._dfs(state)
        return {"opt_makespan": self.best, "schedule": self.best_schedule}

    def _lower_bound(self, state):
        done_work = sum(self.proc[i] for i, s in enumerate(state["status"]) if s == "done")
        running_remaining = sum(state["finish_times"][idx] - state["time"] for idx in state["running"].values())
        waiting_work = self.total_work - done_work - sum(self.proc[idx] for idx in state["running"].values())
        machine_lb = state["time"] + ceil((running_remaining + waiting_work) / self.machine_count)

        path_lb = state["time"]
        for idx, status in enumerate(state["status"]):
            if status == "done":
                continue
            ready_t = self._earliest_ready_time(idx, state["start_times"])
            if ready_t is None:
                continue
            path_lb = max(path_lb, ready_t + self.proc[idx])
        return max(machine_lb, path_lb)

    def _advance_to_time(self, state, next_time):
        new_state = deepcopy(state)
        new_state["time"] = next_time
        completed = []
        for machine, idx in list(new_state["running"].items()):
            if new_state["finish_times"][idx] == next_time:
                new_state["status"][idx] = "done"
                completed.append(machine)
                new_state["done_count"] += 1
        for machine in completed:
            del new_state["running"][machine]
        return new_state

    def _ready_tasks(self, state):
        ready = []
        for idx, status in enumerate(state["status"]):
            if status not in ("waiting", "ready"):
                continue
            ready_t = self._earliest_ready_time(idx, state["start_times"])
            if ready_t is not None and ready_t <= state["time"]:
                ready.append(idx)
        return sorted(ready)

    def _next_event_time(self, state):
        candidates = []
        for idx in state["running"].values():
            candidates.append(state["finish_times"][idx])
        for idx, status in enumerate(state["status"]):
            if status not in ("waiting", "ready"):
                continue
            if self.release[idx] > state["time"]:
                candidates.append(self.release[idx])
            ready_t = self._earliest_ready_time(idx, state["start_times"])
            if ready_t is not None and ready_t > state["time"]:
                candidates.append(ready_t)
        return min(candidates) if candidates else None

    def _start_subset(self, state, subset):
        new_state = deepcopy(state)
        idle_machines = [m for m in range(self.machine_count) if m not in new_state["running"]]
        for machine, idx in zip(idle_machines, subset):
            new_state["status"][idx] = "running"
            new_state["start_times"][idx] = new_state["time"]
            new_state["finish_times"][idx] = new_state["time"] + self.proc[idx]
            new_state["running"][machine] = idx
        return new_state

    def _record_best(self, state):
        makespan = max(ft for ft in state["finish_times"] if ft is not None)
        if makespan < self.best:
            self.best = makespan
            self.best_schedule = {
                self.tasks[idx].id: {
                    "start_time": state["start_times"][idx],
                    "finish_time": state["finish_times"][idx],
                }
                for idx in range(self.n)
            }

    def _dfs(self, state):
        if self._lower_bound(state) >= self.best:
            return

        if state["done_count"] == self.n:
            self._record_best(state)
            return

        ready = self._ready_tasks(state)
        idle_count = self.machine_count - len(state["running"])

        if ready and idle_count > 0:
            k = min(len(ready), idle_count)
            # identical machines + same start time 下，subset 即可，不需排列。
            for subset in combinations(ready, k):
                child = self._start_subset(state, subset)
                self._dfs(child)
            return

        next_time = self._next_event_time(state)
        if next_time is None:
            return
        child = self._advance_to_time(state, next_time)
        self._dfs(child)
