import matplotlib.pyplot as plt
from collections import defaultdict


def plot_gantt(sim, filepath):
    records = sim.build_task_records()

    machine_tasks = defaultdict(list)
    for r in records:
        machine_tasks[r["machine_id"]].append(r)

    fig, ax = plt.subplots(figsize=(12, 6))

    yticks, ylabels = [], []

    colors = {}
    palette = plt.cm.tab20.colors
    job_ids = sorted(set(r["job_id"] for r in records))
    for i, j in enumerate(job_ids):
        colors[j] = palette[i % len(palette)]

    y = 0
    for m, tasks in sorted(machine_tasks.items()):
        for t in tasks:
            start = t["start_time"]
            dur = t["finish_time"] - t["start_time"]

            ax.barh(y, dur, left=start, color=colors[t["job_id"]], edgecolor="black")

            ax.text(start + dur / 2, y, t["task_id"], ha="center", va="center", fontsize=8)

        yticks.append(y)
        ylabels.append(m)
        y += 1

    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels)
    ax.set_xlabel("Time")
    ax.set_title(f"Gantt - {sim.policy}")
    ax.grid(alpha=0.3)

    fig.tight_layout()
    fig.savefig(filepath)
    plt.close(fig)