def fifo_select(ready_tasks):
    # True FIFO should prefer tasks that entered the ready queue earlier.
    ready_tasks.sort(key=lambda x: (x.ready_since, x.job_release_time, x.id))
    return ready_tasks[0]
