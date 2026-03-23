import random
from copy import deepcopy
from model_v2 import Task, Job


# =========================
# V8 設計理念
# 1) 主體 benchmark 改成「中性、可參數化」的 layered DAG / multi-level BOM generator
# 2) 保留少量 mechanism case，專門展示 static rank 與 dynamic bottleneck 的分歧
# 3) 小型 exact family 供 OPT 比對
# =========================


MECHANISM_ARCHETYPES = {
    "anchor": {
        "durations": {"T1": 1, "T2": 4, "T3": 14, "T4": 13, "T5": 12, "T6": 6},
        "edges": [("T1", "T2"), ("T2", "T3"), ("T2", "T4"), ("T3", "T5"), ("T4", "T5"), ("T5", "T6")],
        "delta_map": {("T1", "T2"): 0, ("T2", "T3"): 0, ("T2", "T4"): 0, ("T3", "T5"): 0, ("T4", "T5"): 0, ("T5", "T6"): 0},
    },
    "trap": {
        "durations": {"T1": 1, "T2": 7, "T3": 18, "T4": 17, "T5": 5, "T6": 2},
        "edges": [("T1", "T2"), ("T2", "T3"), ("T2", "T4"), ("T3", "T5"), ("T4", "T5"), ("T5", "T6")],
        "delta_map": {("T1", "T2"): 0, ("T2", "T3"): 12, ("T2", "T4"): 12, ("T3", "T5"): 4, ("T4", "T5"): 4, ("T5", "T6"): 0},
    },
    "urgent": {
        "durations": {"T1": 1, "T2": 2, "T3": 10, "T4": 7, "T5": 2},
        "edges": [("T1", "T2"), ("T2", "T3"), ("T3", "T4"), ("T4", "T5")],
        "delta_map": {("T1", "T2"): 1, ("T2", "T3"): 4, ("T3", "T4"): 0, ("T4", "T5"): 0},
    },
}


BENCHMARK_FAMILIES = {
    "chain_heavy": {
        "depth": 5,
        "width": 2,
        "branch_prob": 0.35,
        "merge_prob": 0.25,
        "cross_level_prob": 0.05,
        "proc_range": (2, 10),
        "root_count_range": (1, 2),
        "level_width_jitter": 0,
        "release_spread": 2,
        "overlap_mode": "low",
        "jobs_per_instance": 4,
    },
    "fork_join": {
        "depth": 4,
        "width": 3,
        "branch_prob": 0.55,
        "merge_prob": 0.45,
        "cross_level_prob": 0.08,
        "proc_range": (2, 9),
        "root_count_range": (1, 2),
        "level_width_jitter": 1,
        "release_spread": 3,
        "overlap_mode": "medium",
        "jobs_per_instance": 4,
    },
    "merge_heavy": {
        "depth": 5,
        "width": 3,
        "branch_prob": 0.3,
        "merge_prob": 0.7,
        "cross_level_prob": 0.05,
        "proc_range": (2, 10),
        "root_count_range": (1, 2),
        "level_width_jitter": 1,
        "release_spread": 3,
        "overlap_mode": "low",
        "jobs_per_instance": 4,
    },
    "overlap_rich": {
        "depth": 5,
        "width": 3,
        "branch_prob": 0.45,
        "merge_prob": 0.45,
        "cross_level_prob": 0.12,
        "proc_range": (2, 9),
        "root_count_range": (1, 2),
        "level_width_jitter": 1,
        "release_spread": 4,
        "overlap_mode": "high",
        "jobs_per_instance": 4,
    },
}


def default_v8_config():
    return {
        "machine_counts": [2, 4, 8],
        "policies": ["FIFO", "HEFT", "BOS"],
        "seeds": [11, 19, 27, 35, 43],
        "families": ["chain_heavy", "fork_join", "merge_heavy", "overlap_rich"],
        "size_profiles": {
            "small": {"jobs_per_instance": 3, "depth": 4, "width": 2},
            "medium": {"jobs_per_instance": 4, "depth": 5, "width": 3},
            "large": {"jobs_per_instance": 6, "depth": 6, "width": 4},
        },
        "mechanism_seed": 11,
        "small_exact_cases": 6,
        "small_exact_machine_count": 2,
        "small_exact_tasks_upper": 8,
    }


def _scaled_duration(base, scale, jitter, rng):
    value = base * scale
    if jitter > 0:
        value += rng.randint(-jitter, jitter)
    return max(1, int(round(value)))


def _make_job_from_template(job_id, release, archetype_name, rng, duration_scale_range=(1.0, 1.0), task_jitter=0, overlap_multiplier=1.0):
    arch = MECHANISM_ARCHETYPES[archetype_name]
    scale = rng.uniform(*duration_scale_range)
    job = Job(job_id, release)
    tasks = {}

    for name, base_p in arch["durations"].items():
        p = _scaled_duration(base_p, scale, task_jitter, rng)
        tasks[name] = Task(f"{job_id}_{name}", job_id, p, release if name == "T1" else 0)

    for src, dst in arch["edges"]:
        tasks[src].succ.append(tasks[dst])
        tasks[dst].pred.append(tasks[src])
        delta = int(round(arch["delta_map"].get((src, dst), 0) * overlap_multiplier))
        delta = max(0, min(delta, tasks[src].p - 1 if tasks[src].p > 1 else 0))
        tasks[src].delta_to_succ[tasks[dst]] = delta

    job.tasks = [tasks[name] for name in sorted(tasks.keys(), key=lambda x: int(x[1:]))]
    return job


def create_mechanism_case(seed=11):
    rng = random.Random(seed)
    specs = [
        ("A", 0, "anchor"),
        ("B", 0, "trap"),
        ("C", 1, "anchor"),
        ("D", 1, "trap"),
        ("U", 4, "urgent"),
        ("V", 6, "urgent"),
    ]
    jobs = []
    for job_id, release, archetype in specs:
        jobs.append(
            _make_job_from_template(
                job_id=job_id,
                release=release,
                archetype_name=archetype,
                rng=rng,
                duration_scale_range=(0.97, 1.03),
                task_jitter=0,
                overlap_multiplier=1.0,
            )
        )
    return jobs


def _sample_overlap_ratio(mode, rng):
    if mode == "low":
        return rng.uniform(0.0, 0.15)
    if mode == "medium":
        return rng.uniform(0.1, 0.35)
    if mode == "high":
        return rng.uniform(0.25, 0.6)
    return rng.uniform(0.0, 0.3)


def _build_layer_widths(depth, width, jitter, root_count_range, rng):
    widths = []
    root_low, root_high = root_count_range
    widths.append(rng.randint(root_low, root_high))
    for _ in range(1, depth):
        w = width + rng.randint(-jitter, jitter) if jitter > 0 else width
        widths.append(max(1, w))
    return widths


def _generate_layered_job(job_id, release_time, family_cfg, rng):
    depth = family_cfg["depth"]
    width = family_cfg["width"]
    proc_low, proc_high = family_cfg["proc_range"]
    widths = _build_layer_widths(
        depth=depth,
        width=width,
        jitter=family_cfg.get("level_width_jitter", 0),
        root_count_range=family_cfg.get("root_count_range", (1, 1)),
        rng=rng,
    )

    job = Job(job_id, release_time)
    levels = []
    task_index = 1
    for lv, lv_width in enumerate(widths):
        cur_level = []
        for pos in range(lv_width):
            p = rng.randint(proc_low, proc_high)
            task = Task(f"{job_id}_T{task_index}", job_id, p, release_time if lv == 0 else 0)
            task.level = lv
            task.level_pos = pos
            cur_level.append(task)
            task_index += 1
        levels.append(cur_level)

    branch_prob = family_cfg["branch_prob"]
    merge_prob = family_cfg["merge_prob"]
    cross_level_prob = family_cfg.get("cross_level_prob", 0.0)
    overlap_mode = family_cfg.get("overlap_mode", "medium")

    # 每個非 root 節點至少有一個 predecessor；並以 merge_prob 控制額外 predecessor 數量。
    for lv in range(1, depth):
        prev_nodes = levels[lv - 1]
        cur_nodes = levels[lv]
        for node in cur_nodes:
            pred_count = 1
            for _ in prev_nodes:
                if rng.random() < merge_prob:
                    pred_count += 1
            pred_count = min(len(prev_nodes), pred_count)
            preds = rng.sample(prev_nodes, pred_count)
            for pred in preds:
                if node not in pred.succ:
                    pred.succ.append(node)
                    node.pred.append(pred)

        # branch：讓上一層節點可能連到多個後繼
        for pred in prev_nodes:
            for node in cur_nodes:
                if node in pred.succ:
                    continue
                if rng.random() < branch_prob:
                    pred.succ.append(node)
                    node.pred.append(pred)

        # 少量跨層 edge，增加 residual graph 多樣性
        if lv >= 2 and cross_level_prob > 0:
            earlier_nodes = [t for earlier in levels[: lv - 1] for t in earlier]
            for node in cur_nodes:
                if rng.random() < cross_level_prob and earlier_nodes:
                    pred = rng.choice(earlier_nodes)
                    if node not in pred.succ:
                        pred.succ.append(node)
                        node.pred.append(pred)

    # 設定 edge-wise overlap allowance
    for lv_nodes in levels:
        for pred in lv_nodes:
            for succ in pred.succ:
                ratio = _sample_overlap_ratio(overlap_mode, rng)
                delta = int(round(pred.p * ratio))
                delta = max(0, min(delta, pred.p - 1 if pred.p > 1 else 0))
                pred.delta_to_succ[succ] = delta

    # 清理重複 edge，避免 generator 疊加。
    for lv_nodes in levels:
        for node in lv_nodes:
            node.succ = list(dict.fromkeys(node.succ))
            node.pred = list(dict.fromkeys(node.pred))

    job.tasks = [task for lv_nodes in levels for task in lv_nodes]
    return job


def create_benchmark_instance(family_name, size_name, seed):
    family_cfg = deepcopy(BENCHMARK_FAMILIES[family_name])
    size_profile = default_v8_config()["size_profiles"][size_name]
    family_cfg.update(size_profile)

    rng = random.Random(seed)
    jobs = []
    release_spread = family_cfg.get("release_spread", 0)
    jobs_per_instance = family_cfg["jobs_per_instance"]

    for j in range(jobs_per_instance):
        release_time = rng.randint(0, release_spread)
        jobs.append(_generate_layered_job(f"J{j+1}", release_time, family_cfg, rng))
    return jobs, family_cfg


def create_exact_small_instances(case_count=6, seed=101):
    rng = random.Random(seed)
    cases = []
    family_names = ["chain_heavy", "fork_join", "merge_heavy"]
    for idx in range(case_count):
        family_name = family_names[idx % len(family_names)]
        family_cfg = deepcopy(BENCHMARK_FAMILIES[family_name])
        family_cfg.update({
            "depth": 3,
            "width": 2,
            "jobs_per_instance": 2,
            "proc_range": (1, 6),
            "release_spread": 2,
            "level_width_jitter": 0,
            "cross_level_prob": 0.0,
        })
        local_seed = rng.randint(1, 10**6)
        local_rng = random.Random(local_seed)
        jobs = []
        for j in range(family_cfg["jobs_per_instance"]):
            release_time = local_rng.randint(0, family_cfg["release_spread"])
            jobs.append(_generate_layered_job(f"S{idx+1}_{j+1}", release_time, family_cfg, local_rng))
        cases.append({
            "case_id": f"exact_case_{idx+1}",
            "family": family_name,
            "seed": local_seed,
            "jobs": jobs,
            "config": family_cfg,
        })
    return cases
