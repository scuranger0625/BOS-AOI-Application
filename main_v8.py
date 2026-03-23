from copy import deepcopy
from pathlib import Path

from simulator_v8 import SimulatorV8
from exact_solver_v8 import ExactOptimalSolver
from scenario_v8 import default_v8_config, create_benchmark_instance, create_mechanism_case, create_exact_small_instances
from report_v8 import (
    ensure_dir,
    write_csv,
    write_json,
    compute_path_metrics,
    build_summary,
    disagreement_rate,
    aggregate_mean,
    plot_family_makespan,
    plot_gap_vs_lb,
)
from gantt_v5 import plot_gantt


def clone_jobs(jobs):
    # deepcopy 就夠，因為 Task/Job 結構單純。
    return deepcopy(jobs)


def run_single(policy, jobs, machine_count, experiment_name, seed, family=None, size_name=None, opt_makespan=None, config=None):
    structure_metrics = compute_path_metrics(jobs)
    sim = SimulatorV8(jobs, policy=policy, machine_count=machine_count, verbose=False)
    sim.run()
    summary = build_summary(
        sim,
        machine_count=machine_count,
        experiment_name=experiment_name,
        seed=seed,
        family=family,
        size_name=size_name,
        structure_metrics=structure_metrics,
        opt_makespan=opt_makespan,
        config=config,
    )
    return sim, summary


def benchmark_suite(output_dir, cfg):
    detail_rows = []
    mechanism_rows = []
    benchmark_runs = []

    for family in cfg["families"]:
        for size_name in cfg["size_profiles"].keys():
            for machine_count in cfg["machine_counts"]:
                for seed in cfg["seeds"]:
                    base_jobs, family_cfg = create_benchmark_instance(family, size_name, seed)
                    run_cache = {}
                    for policy in cfg["policies"]:
                        jobs = clone_jobs(base_jobs)
                        sim, summary = run_single(
                            policy=policy,
                            jobs=jobs,
                            machine_count=machine_count,
                            experiment_name="benchmark_v8",
                            seed=seed,
                            family=family,
                            size_name=size_name,
                            config={**family_cfg, "machine_count": machine_count},
                        )
                        detail_rows.append(summary)
                        run_cache[policy] = sim
                        benchmark_runs.append((family, size_name, machine_count, seed, policy, sim, summary))

                    mechanism_rows.append({
                        "family": family,
                        "size_name": size_name,
                        "machine_count": machine_count,
                        "seed": seed,
                        "bos_vs_heft_disagreement": disagreement_rate(run_cache["BOS"].decision_log, run_cache["HEFT"].decision_log),
                        "bos_vs_fifo_disagreement": disagreement_rate(run_cache["BOS"].decision_log, run_cache["FIFO"].decision_log),
                    })

    summary_rows = aggregate_mean(
        rows=detail_rows,
        group_keys=["family", "size_name", "machine_count", "policy"],
        metric_keys=["makespan", "machine_utilization", "gap_vs_lb_pct", "idle_machine_time", "non_full_duration"],
    )
    mechanism_summary = aggregate_mean(
        rows=mechanism_rows,
        group_keys=["family", "size_name", "machine_count"],
        metric_keys=["bos_vs_heft_disagreement", "bos_vs_fifo_disagreement"],
    )

    write_csv(detail_rows, output_dir / "v8_benchmark_detail.csv")
    write_csv(summary_rows, output_dir / "v8_benchmark_summary.csv")
    write_csv(mechanism_rows, output_dir / "v8_decision_disagreement_detail.csv")
    write_csv(mechanism_summary, output_dir / "v8_decision_disagreement_summary.csv")
    plot_family_makespan(summary_rows, output_dir / "v8_benchmark_makespan.png")
    plot_gap_vs_lb(summary_rows, output_dir / "v8_gap_vs_lb.png")

    return detail_rows, summary_rows, mechanism_rows, mechanism_summary, benchmark_runs


def mechanism_case_study(output_dir, cfg):
    seed = cfg["mechanism_seed"]
    base_jobs = create_mechanism_case(seed=seed)
    rows = []
    sims = {}
    machine_count = 2
    for policy in cfg["policies"]:
        sim, summary = run_single(
            policy=policy,
            jobs=clone_jobs(base_jobs),
            machine_count=machine_count,
            experiment_name="mechanism_case_v8",
            seed=seed,
            family="mechanism_case",
            size_name="case_study",
            config={"machine_count": machine_count},
        )
        rows.append(summary)
        sims[policy] = sim
        write_csv(sim.build_task_records(), output_dir / f"v8_mechanism_{policy.lower()}_tasks.csv")
        write_json(sim.decision_log, output_dir / f"v8_mechanism_{policy.lower()}_decisions.json")
        plot_gantt(sim, output_dir / f"v8_mechanism_{policy.lower()}_gantt.png")

    write_csv(rows, output_dir / "v8_mechanism_summary.csv")
    write_json({
        "bos_vs_heft_disagreement": disagreement_rate(sims["BOS"].decision_log, sims["HEFT"].decision_log),
        "bos_vs_fifo_disagreement": disagreement_rate(sims["BOS"].decision_log, sims["FIFO"].decision_log),
    }, output_dir / "v8_mechanism_disagreement.json")
    return rows


def exact_small_suite(output_dir, cfg):
    cases = create_exact_small_instances(case_count=cfg["small_exact_cases"], seed=2026)
    detail_rows = []
    for case in cases:
        jobs = case["jobs"]
        solver = ExactOptimalSolver(clone_jobs(jobs), machine_count=cfg["small_exact_machine_count"])
        exact = solver.solve()
        opt_makespan = exact["opt_makespan"]

        for policy in cfg["policies"]:
            sim, summary = run_single(
                policy=policy,
                jobs=clone_jobs(jobs),
                machine_count=cfg["small_exact_machine_count"],
                experiment_name="exact_small_v8",
                seed=case["seed"],
                family=case["family"],
                size_name=case["case_id"],
                opt_makespan=opt_makespan,
                config=case["config"],
            )
            summary["case_id"] = case["case_id"]
            detail_rows.append(summary)

    summary_rows = aggregate_mean(
        rows=detail_rows,
        group_keys=["family", "policy"],
        metric_keys=["makespan", "gap_vs_opt_pct", "gap_vs_lb_pct"],
    )
    write_csv(detail_rows, output_dir / "v8_exact_detail.csv")
    write_csv(summary_rows, output_dir / "v8_exact_summary.csv")
    return detail_rows, summary_rows


def print_key_tables(summary_rows, mechanism_summary, exact_summary):
    print("\n=== V8 Benchmark Summary ===")
    for row in sorted(summary_rows, key=lambda r: (r["family"], r["size_name"], r["machine_count"], r["policy"])):
        print(
            f"{row['family']:<12} | {row['size_name']:<6} | m={row['machine_count']} | {row['policy']:<4} "
            f"| avg_makespan={row['avg_makespan']:>7} | avg_gap_lb={row['avg_gap_vs_lb_pct']:>7}%"
        )

    print("\n=== V8 Decision Disagreement ===")
    for row in sorted(mechanism_summary, key=lambda r: (r["family"], r["size_name"], r["machine_count"])):
        print(
            f"{row['family']:<12} | {row['size_name']:<6} | m={row['machine_count']} "
            f"| BOS vs HEFT={row['avg_bos_vs_heft_disagreement']} | BOS vs FIFO={row['avg_bos_vs_fifo_disagreement']}"
        )

    print("\n=== V8 Exact Small Summary ===")
    for row in sorted(exact_summary, key=lambda r: (r["family"], r["policy"])):
        print(
            f"{row['family']:<12} | {row['policy']:<4} | avg_gap_opt={row.get('avg_gap_vs_opt_pct', None)} "
            f"| avg_gap_lb={row.get('avg_gap_vs_lb_pct', None)}"
        )


def main():
    cfg = default_v8_config()
    output_dir = ensure_dir(Path("v8_outputs"))

    benchmark_detail, benchmark_summary, mechanism_detail, mechanism_summary, _ = benchmark_suite(output_dir, cfg)
    case_rows = mechanism_case_study(output_dir, cfg)
    exact_detail, exact_summary = exact_small_suite(output_dir, cfg)

    final_summary = {
        "version": "v8",
        "benchmark_summary": benchmark_summary,
        "decision_disagreement_summary": mechanism_summary,
        "mechanism_case": case_rows,
        "exact_summary": exact_summary,
    }
    write_json(final_summary, output_dir / "v8_summary.json")
    print_key_tables(benchmark_summary, mechanism_summary, exact_summary)
    print(f"\n✅ V8 completed\n📁 Outputs: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
