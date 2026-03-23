from pathlib import Path
import csv
import json
from collections import defaultdict
import matplotlib.pyplot as plt


def ensure_dir(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_csv(rows, filepath):
    filepath = Path(filepath)
    if not rows:
        filepath.write_text("", encoding="utf-8")
        return filepath
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with filepath.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return filepath


def write_json(obj, filepath):
    filepath = Path(filepath)
    filepath.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return filepath


def compute_path_metrics(jobs):
    tasks = [task for job in jobs for task in job.tasks]
    task_to_idx = {task: idx for idx, task in enumerate(tasks)}
    indeg = {task: 0 for task in tasks}
    for task in tasks:
        for succ in task.succ:
            indeg[succ] += 1

    q = [task for task in tasks if indeg[task] == 0]
    topo = []
    while q:
        node = q.pop(0)
        topo.append(node)
        for succ in node.succ:
            indeg[succ] -= 1
            if indeg[succ] == 0:
                q.append(succ)

    cp = {}
    ldelta = {}
    for task in reversed(topo):
        if not task.succ:
            cp[task] = task.p
            ldelta[task] = task.p
        else:
            cp[task] = task.p + max(cp[succ] for succ in task.succ)
            ldelta[task] = task.p + max(ldelta[succ] - task.delta_to_succ.get(succ, 0) for succ in task.succ)
    return {
        "task_count": len(tasks),
        "total_processing": sum(t.p for t in tasks),
        "critical_path": max(cp.values()) if cp else 0,
        "L_delta": max(ldelta.values()) if ldelta else 0,
        "edge_count": sum(len(t.succ) for t in tasks),
        "avg_overlap": round(
            sum(delta for task in tasks for delta in task.delta_to_succ.values()) /
            max(1, sum(len(task.delta_to_succ) for task in tasks)),
            4,
        ),
    }


def build_summary(sim, machine_count, experiment_name=None, seed=None, family=None, size_name=None, structure_metrics=None, opt_makespan=None, config=None):
    records = sim.build_task_records()
    makespan = max(row["finish_time"] for row in records)
    total_processing = sum(row["processing_time"] for row in records)
    utilization = total_processing / (machine_count * makespan) if makespan else 0
    structure_metrics = structure_metrics or {}
    lb = max(total_processing / machine_count if machine_count else 0, structure_metrics.get("L_delta", 0))
    out = {
        "experiment": experiment_name,
        "seed": seed,
        "family": family,
        "size_name": size_name,
        "policy": sim.policy,
        "machine_count": machine_count,
        "makespan": makespan,
        "total_processing_time": total_processing,
        "machine_utilization": round(utilization, 4),
        "critical_path": structure_metrics.get("critical_path"),
        "L_delta": structure_metrics.get("L_delta"),
        "task_count": structure_metrics.get("task_count"),
        "edge_count": structure_metrics.get("edge_count"),
        "avg_overlap": structure_metrics.get("avg_overlap"),
        "lb_w_over_m": round(total_processing / machine_count, 4) if machine_count else None,
        "lower_bound": round(lb, 4),
        "gap_vs_lb_pct": round((makespan - lb) / lb * 100, 4) if lb else None,
        "idle_machine_time": sim.idle_breakdown["idle_machine_time"],
        "non_full_duration": sim.idle_breakdown["non_full_duration"],
        "release_blocked_duration": sim.idle_breakdown["release_blocked_duration"],
        "precedence_blocked_duration": sim.idle_breakdown["precedence_blocked_duration"],
        "mixed_blocked_duration": sim.idle_breakdown["mixed_blocked_duration"],
    }
    if opt_makespan is not None:
        out["opt_makespan"] = opt_makespan
        out["gap_vs_opt_pct"] = round((makespan - opt_makespan) / opt_makespan * 100, 4) if opt_makespan else None
    if config is not None:
        out["config"] = config
    return out


def disagreement_rate(decision_log_a, decision_log_b):
    by_time_a = defaultdict(list)
    by_time_b = defaultdict(list)
    for row in decision_log_a:
        by_time_a[row["time"]].append(row["selected_task"])
    for row in decision_log_b:
        by_time_b[row["time"]].append(row["selected_task"])
    common_times = sorted(set(by_time_a) & set(by_time_b))
    if not common_times:
        return 0.0
    total = 0
    diff = 0
    for t in common_times:
        seq_a = by_time_a[t]
        seq_b = by_time_b[t]
        for a, b in zip(seq_a, seq_b):
            total += 1
            if a != b:
                diff += 1
    return round(diff / total, 4) if total else 0.0


def aggregate_mean(rows, group_keys, metric_keys):
    grouped = {}
    counts = {}
    for row in rows:
        key = tuple(row[k] for k in group_keys)
        if key not in grouped:
            grouped[key] = {k: row[k] for k in group_keys}
            for metric in metric_keys:
                grouped[key][metric] = 0.0
            counts[key] = 0
        counts[key] += 1
        for metric in metric_keys:
            value = row.get(metric)
            if value is not None:
                grouped[key][metric] += value

    out = []
    for key, row in grouped.items():
        count = counts[key]
        new_row = dict(row)
        new_row["sample_count"] = count
        for metric in metric_keys:
            new_row[f"avg_{metric}"] = round(row[metric] / count, 4)
        out.append(new_row)
    return out


def plot_family_makespan(summary_rows, filepath):
    grouped = defaultdict(lambda: defaultdict(dict))
    for row in summary_rows:
        grouped[(row["family"], row["size_name"], row["machine_count"])][row["policy"]] = row["avg_makespan"]

    labels = []
    fifo = []
    heft = []
    bos = []
    for key in sorted(grouped.keys()):
        labels.append(f"{key[0]}\n{key[1]}\nm={key[2]}")
        fifo.append(grouped[key].get("FIFO", 0))
        heft.append(grouped[key].get("HEFT", 0))
        bos.append(grouped[key].get("BOS", 0))

    x = list(range(len(labels)))
    width = 0.25
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar([i - width for i in x], fifo, width=width, label="FIFO")
    ax.bar(x, heft, width=width, label="HEFT")
    ax.bar([i + width for i in x], bos, width=width, label="BOS")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Average makespan")
    ax.set_title("V8 Benchmark Summary by Family / Size / Machine Count")
    ax.grid(alpha=0.3, axis="y")
    ax.legend()
    fig.tight_layout()
    fig.savefig(filepath, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return filepath


def plot_gap_vs_lb(summary_rows, filepath):
    grouped = defaultdict(lambda: defaultdict(dict))
    for row in summary_rows:
        grouped[(row["family"], row["size_name"], row["machine_count"])][row["policy"]] = row["avg_gap_vs_lb_pct"]

    labels = []
    fifo = []
    heft = []
    bos = []
    for key in sorted(grouped.keys()):
        labels.append(f"{key[0]}\n{key[1]}\nm={key[2]}")
        fifo.append(grouped[key].get("FIFO", 0))
        heft.append(grouped[key].get("HEFT", 0))
        bos.append(grouped[key].get("BOS", 0))

    x = list(range(len(labels)))
    width = 0.25
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar([i - width for i in x], fifo, width=width, label="FIFO")
    ax.bar(x, heft, width=width, label="HEFT")
    ax.bar([i + width for i in x], bos, width=width, label="BOS")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Average gap vs lower bound (%)")
    ax.set_title("V8 Gap-to-LB Comparison")
    ax.grid(alpha=0.3, axis="y")
    ax.legend()
    fig.tight_layout()
    fig.savefig(filepath, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return filepath
