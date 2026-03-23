from scheduler_bos_v6 import compute_D_full, update_D_from, bos_v6_select
from scheduler_fifo_v2 import fifo_select
from scheduler_heft_v4 import heft_select
from model_v2 import Machine


class SimulatorV8:
    def __init__(self, jobs, policy="FIFO", machine_count=2, verbose=False):
        self.initialized_D = False
        self.jobs = jobs
        self.policy = policy.upper()
        self.time = 0
        self.verbose = verbose

        self.machines = [Machine(f"M{i + 1}") for i in range(machine_count)]
        self.all_tasks = [task for job in jobs for task in job.tasks]

        self.event_log = []
        self.assignment_log = []
        self.decision_log = []
        self.interval_log = []
        self.idle_breakdown = {
            "non_full_duration": 0,
            "idle_machine_time": 0,
            "release_blocked_duration": 0,
            "precedence_blocked_duration": 0,
            "mixed_blocked_duration": 0,
            "no_waiting_tasks_duration": 0,
        }

    def log(self, message):
        if self.verbose:
            print(message)

    def record_event(self, event_type, **payload):
        row = {"time": self.time, "event": event_type}
        row.update(payload)
        self.event_log.append(row)

    def all_done(self):
        return all(task.status == "done" for task in self.all_tasks)

    def process_completions(self):
        for machine in self.machines:
            task = machine.current_task
            if task is None:
                continue
            if task.finish_time == self.time:
                task.status = "done"
                machine.current_task = None
                if self.policy == "BOS":
                    update_D_from(task)
                self.record_event(
                    "complete",
                    task_id=task.id,
                    job_id=task.job_id,
                    machine_id=machine.id,
                    start_time=task.start_time,
                    finish_time=task.finish_time,
                )

    def refresh_ready_tasks(self):
        ready = []
        for task in self.all_tasks:
            if task.is_ready(self.time):
                if task.status == "waiting":
                    task.status = "ready"
                    task.ready_since = self.time
                    self.record_event(
                        "activate",
                        task_id=task.id,
                        job_id=task.job_id,
                        ready_since=task.ready_since,
                        earliest_ready_time=task.earliest_ready_time(),
                    )
                ready.append(task)
        return ready

    def select_task(self, ready_tasks):
        ready_snapshot = list(ready_tasks)
        if self.policy == "BOS":
            task = bos_v6_select(ready_tasks)
            candidates = [
                {"task_id": t.id, "priority": t.D, "priority_name": "D", "ready_since": t.ready_since}
                for t in sorted(ready_snapshot, key=lambda x: (-x.D, x.ready_since, x.id))
            ]
        elif self.policy == "HEFT":
            task = heft_select(ready_tasks)
            candidates = [
                {
                    "task_id": t.id,
                    "priority": getattr(t, "static_rank", t.p),
                    "priority_name": "static_rank",
                    "ready_since": t.ready_since,
                }
                for t in sorted(ready_snapshot, key=lambda x: (-getattr(x, "static_rank", x.p), x.ready_since, x.id))
            ]
        else:
            task = fifo_select(ready_tasks)
            candidates = [
                {
                    "task_id": t.id,
                    "priority": t.ready_since,
                    "priority_name": "ready_since",
                    "ready_since": t.ready_since,
                }
                for t in sorted(ready_snapshot, key=lambda x: (x.ready_since, x.job_release_time, x.id))
            ]

        self.decision_log.append({
            "time": self.time,
            "policy": self.policy,
            "selected_task": task.id,
            "ready_candidates": candidates,
        })
        return task

    def assign(self, task, machine):
        task.status = "running"
        task.start_time = self.time
        task.finish_time = self.time + task.p
        task.assigned_machine = machine
        machine.current_task = task

        row = {
            "time": self.time,
            "task_id": task.id,
            "job_id": task.job_id,
            "machine_id": machine.id,
            "processing_time": task.p,
            "ready_since": task.ready_since,
            "start_time": task.start_time,
            "finish_time": task.finish_time,
            "policy": self.policy,
            "D": getattr(task, "D", None),
            "static_rank": getattr(task, "static_rank", None),
        }
        self.assignment_log.append(row)
        self.record_event("assign", **row)

    def _blocked_reason_snapshot(self):
        waiting = [t for t in self.all_tasks if t.status in ("waiting", "ready")]
        if not waiting:
            return "no_waiting"

        has_release_block = False
        has_precedence_block = False
        for task in waiting:
            if task.status == "ready":
                return "ready_exists"
            if self.time < task.job_release_time:
                has_release_block = True
                continue
            ready_t = task.earliest_ready_time()
            if ready_t is None or ready_t > self.time:
                has_precedence_block = True

        if has_release_block and has_precedence_block:
            return "mixed"
        if has_release_block:
            return "release"
        if has_precedence_block:
            return "precedence"
        return "unknown"

    def _record_interval(self, next_time):
        dt = next_time - self.time
        if dt <= 0:
            return
        idle_count = sum(1 for m in self.machines if m.current_task is None)
        running_count = len(self.machines) - idle_count
        reason = self._blocked_reason_snapshot() if idle_count > 0 else "full"
        self.interval_log.append({
            "start": self.time,
            "end": next_time,
            "duration": dt,
            "idle_machines": idle_count,
            "running_machines": running_count,
            "reason": reason,
        })
        if idle_count > 0:
            self.idle_breakdown["non_full_duration"] += dt
            self.idle_breakdown["idle_machine_time"] += dt * idle_count
            if reason == "release":
                self.idle_breakdown["release_blocked_duration"] += dt
            elif reason == "precedence":
                self.idle_breakdown["precedence_blocked_duration"] += dt
            elif reason == "mixed":
                self.idle_breakdown["mixed_blocked_duration"] += dt
            elif reason == "no_waiting":
                self.idle_breakdown["no_waiting_tasks_duration"] += dt

    def next_event_time(self):
        candidate_times = []
        for machine in self.machines:
            task = machine.current_task
            if task is not None and task.finish_time > self.time:
                candidate_times.append(task.finish_time)
        for task in self.all_tasks:
            if task.status not in ("waiting", "ready"):
                continue
            ready_t = task.earliest_ready_time()
            if ready_t is not None and ready_t > self.time:
                candidate_times.append(ready_t)
            if task.status == "waiting" and task.job_release_time > self.time:
                candidate_times.append(task.job_release_time)
        return min(candidate_times) if candidate_times else None

    def build_task_records(self):
        rows = []
        sorted_tasks = sorted(self.all_tasks, key=lambda x: ((x.start_time if x.start_time is not None else 10**9), x.job_id, x.id))
        for task in sorted_tasks:
            rows.append({
                "task_id": task.id,
                "job_id": task.job_id,
                "machine_id": task.assigned_machine.id if task.assigned_machine else None,
                "processing_time": task.p,
                "ready_since": task.ready_since,
                "start_time": task.start_time,
                "finish_time": task.finish_time,
                "job_release_time": task.job_release_time,
                "policy": self.policy,
                "D": getattr(task, "D", None),
                "static_rank": getattr(task, "static_rank", None),
            })
        return rows

    def run(self):
        if self.policy == "BOS" and not self.initialized_D:
            compute_D_full(self.all_tasks)
            self.initialized_D = True

        while not self.all_done():
            self.process_completions()
            while True:
                ready = self.refresh_ready_tasks()
                idle_machines = [m for m in self.machines if m.current_task is None]
                if not ready or not idle_machines:
                    break
                task = self.select_task(ready)
                machine = idle_machines[0]
                self.assign(task, machine)

            if self.all_done():
                break

            next_time = self.next_event_time()
            if next_time is None:
                raise RuntimeError("Simulation is stuck: no future activation/completion event exists.")
            self._record_interval(next_time)
            self.record_event("jump", from_time=self.time, to_time=next_time)
            self.time = next_time

        makespan = max(task.finish_time for task in self.all_tasks)
        self.record_event("finish", makespan=makespan, policy=self.policy)
        return makespan
