class Task:
    def __init__(self, id, job_id, p, release_time=0):
        self.id = id
        self.job_id = job_id
        self.p = p
        self.release_time = release_time
        self.job_release_time = release_time

        self.pred = []
        self.succ = []
        self.delta_to_succ = {}

        self.status = "waiting"
        self.start_time = None
        self.finish_time = None
        self.assigned_machine = None
        self.ready_since = None
        self.D = 0

    def predecessor_activation_time(self, pred_task):
        """Earliest time this task may start with respect to one predecessor.
        s_i >= s_j + p_j - delta_ji
        """
        if pred_task.start_time is None:
            return None
        delta = pred_task.delta_to_succ.get(self, 0)
        return pred_task.start_time + pred_task.p - delta

    def earliest_ready_time(self):
        """Earliest time the task can become executable under overlap semantics.
        Returns None if some predecessor has not started yet.
        """
        t = self.job_release_time
        for pred_task in self.pred:
            pred_ready_t = self.predecessor_activation_time(pred_task)
            if pred_ready_t is None:
                return None
            t = max(t, pred_ready_t)
        return t

    def is_ready(self, current_time):
        if self.status not in ("waiting", "ready"):
            return False

        ready_t = self.earliest_ready_time()
        return ready_t is not None and current_time >= ready_t


class Job:
    def __init__(self, job_id, release_time):
        self.id = job_id
        self.release_time = release_time
        self.tasks = []


class Machine:
    def __init__(self, id):
        self.id = id
        self.current_task = None
