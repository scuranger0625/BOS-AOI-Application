# BOS V8 實驗設計說明

這版 V8 的核心目標不是再做一個「幫 BOS 贏的劇本」，而是把實驗重新整理成三層：

1. **Theory-aligned benchmark**
   - 回報 `W/m`、`L_delta`、`Cmax`
   - 直接看 `gap_vs_lb_pct`
   - 比較 FIFO / HEFT / BOS 在不同 DAG family、size、machine count 下的表現

2. **Mechanism case study**
   - 保留 anchor / trap / urgent 這種案例
   - 但只當作解釋機制的 case study，不當主 benchmark
   - 輸出 gantt、decision log、task records

3. **Small exact suite**
   - 小型 instance 用 branch-and-bound 求 OPT
   - 回報 `gap_vs_opt_pct`
   - 讓 BOS 的近似敘事不是只停在 heuristic 對打

## 主要檔案

- `scenario_v8.py`：V8 benchmark / mechanism / exact instance generator
- `simulator_v8.py`：event-driven simulator + idle breakdown
- `exact_solver_v8.py`：小型 exact solver
- `report_v8.py`：LB / 統計 / 圖表 / disagreement 指標
- `main_v8.py`：整體實驗入口

## 執行方式

```bash
python main_v8.py
```

輸出目錄：

```bash
v8_outputs/
```

## 你可以直接拿去寫論文的重點指標

- `makespan`
- `lower_bound = max(W/m, L_delta)`
- `gap_vs_lb_pct`
- `gap_vs_opt_pct`（小型 exact）
- `idle_machine_time`
- `non_full_duration`
- `release_blocked_duration`
- `precedence_blocked_duration`
- `bos_vs_heft_disagreement`

## 一句話定位

V8 是把 BOS 從「scenario-driven prototype」往「theory-aligned scheduling study」推進一版。
