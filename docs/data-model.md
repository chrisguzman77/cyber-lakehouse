# Data Model

Reference for every table the pipeline writes. Local tables are path-based
Delta tables under `data/`; on Databricks they are Unity Catalog managed tables
in the `cyber_lakehouse` catalog.

## Table overview

| Layer | Local path | Unity Catalog name | Written by | Write pattern | Key | CDF |
|---|---|---|---|---|---|---|
| Landing | `data/landing/incoming/` (CSV files) | Volume `bronze.landing` → `incoming/` | replay | atomic file rename | file name | — |
| Bronze | `data/bronze/traffic_delta` | `bronze.traffic` | 01 | streaming append | none (raw) | no |
| Silver | `data/silver/traffic_clean` | `silver.traffic_clean` | 02 | insert-only MERGE | `flow_hash` | yes |
| Gold features | `data/gold/features` | `gold.features` | 03 | insert-only MERGE | `flow_hash` | yes |
| Gold analytics | `data/gold/analytics` | `gold.analytics` | 03 | batch overwrite | `attack_cat_norm` | no |
| Frequency encodings | `data/gold/freq_encodings` | `gold.freq_encodings` | 04 | batch overwrite | (`column`, `value`) | no |
| Model metrics | `reports/metrics.json` | `gold.model_metrics`, `gold.family_model_metrics` | 04 | overwrite | `model` | no |
| Model artifacts | `models/` | Volume `gold.artifacts` | 04 | overwrite | — | — |
| Predictions | `data/gold/predictions` | `gold.predictions` | 05 | insert-only MERGE | `flow_hash` | yes |

Stream checkpoints: `data/checkpoints/{bronze,silver,gold_features,predictions}`
locally; `/Volumes/cyber_lakehouse/bronze/checkpoints/<same names>` on
Databricks.

## Bronze — raw flows

The 49 UNSW-NB15 columns exactly as delivered (schema in
`src/utils/schema.py`), plus ingest metadata. The CSVs have no header row.

| Group | Columns | Type |
|---|---|---|
| Flow identity | `srcip`, `sport`, `dstip`, `dsport`, `proto` | string (ports are strings: they contain hex like `0x000b` and `-`) |
| State / service | `state`, `service` | string |
| Timing | `dur`, `sjit`, `djit`, `sintpkt`, `dintpkt`, `tcprtt`, `synack`, `ackdat` | double |
| Time window | `stime`, `ltime` | long (epoch seconds) |
| Volume | `sbytes`, `dbytes`, `spkts`, `dpkts`, `sloss`, `dloss`, `smeansz`, `dmeansz`, `trans_depth`, `res_bdy_len` | long |
| Rates | `sload`, `dload` | double |
| TCP | `sttl`, `dttl`, `swin`, `dwin`, `stcpb`, `dtcpb` | long |
| Context counters | `is_sm_ips_ports`, `ct_state_ttl`, `ct_flw_http_mthd`, `is_ftp_login`, `ct_srv_src`, `ct_srv_dst`, `ct_dst_ltm`, `ct_src_ltm`, `ct_src_dport_ltm`, `ct_dst_sport_ltm`, `ct_dst_src_ltm` | long |
| Quirky counter | `ct_ftp_cmd` | string (contains blanks; cast in silver) |
| Labels | `attack_cat` (string, null/blank = benign), `label` (long, 0/1) | |
| Metadata | `ingest_ts` (timestamp of the micro-batch), `source_file` (landing file path) | |

Full dataset: **2,540,047 rows** across the 4 CSVs.

## Silver — cleaned, unique flows

All bronze columns, with these changes and additions (`src/utils/cleaning.py`):

| Column | Rule |
|---|---|
| `label_int` | `label` cast to int; rows where it is null are dropped |
| `attack_cat_norm` | `attack_cat` trimmed and lowercased; `backdoors` → `backdoor`; null/blank → `normal`. One of 10 values: normal, generic, exploits, fuzzers, dos, reconnaissance, analysis, backdoor, shellcode, worms |
| `ct_ftp_cmd` | `try_cast(trim(ct_ftp_cmd) AS BIGINT)`, null → 0 |
| `ct_flw_http_mthd`, `is_ftp_login` | null → 0 |
| `dur` | rows with `dur < 0` are dropped |
| `flow_hash` | SHA-256 of `srcip‖sport‖dstip‖dsport‖proto‖stime‖ltime‖sbytes‖dbytes` — the flow's identity ([ADR-0003](adr/0003-sha256-flow-identity-key.md)) |
| `is_attack` | `label_int == 1` as int (the model target) |

Unique on `flow_hash`; the first copy of a flow wins (earliest `ingest_ts`,
then `source_file`). Full dataset: **2,013,620 rows**. The ~20.7% of raw rows
removed are duplicate flows plus corrupt rows; duplicates are concentrated in
attack traffic (especially `generic`), which is why the attack share drops from
~13% raw to ~4% in silver.

## Gold features — ML feature table

One row per silver flow. Columns:

- Keys and labels: `flow_hash`, `is_attack`, `attack_cat_norm`, `ingest_ts`
- Categoricals kept as strings: `proto`, `service`, `state` (encoded at
  train/score time — see below)
- **38 raw numeric features** (`RAW_FEATURE_COLS` in `src/utils/features.py`):
  `dur`, `sbytes`, `dbytes`, `sttl`, `dttl`, `sloss`, `dloss`, `sload`,
  `dload`, `spkts`, `dpkts`, `swin`, `dwin`, `stcpb`, `dtcpb`, `smeansz`,
  `dmeansz`, `trans_depth`, `res_bdy_len`, `sjit`, `djit`, `sintpkt`,
  `dintpkt`, `tcprtt`, `synack`, `ackdat`, `is_sm_ips_ports`, `ct_state_ttl`,
  `ct_flw_http_mthd`, `is_ftp_login`, `ct_ftp_cmd`, `ct_srv_src`,
  `ct_srv_dst`, `ct_dst_ltm`, `ct_src_ltm`, `ct_src_dport_ltm`,
  `ct_dst_sport_ltm`, `ct_dst_src_ltm`
- **13 derived features** (all double):

| Feature | Definition |
|---|---|
| `bytes_per_pkt_src` | `sbytes / max(spkts, 1)` |
| `bytes_per_pkt_dst` | `dbytes / max(dpkts, 1)` |
| `byte_ratio_src_dst` | `sbytes / (dbytes + 1)` |
| `pkt_ratio_src_dst` | `spkts / (dpkts + 1)` |
| `load_asymmetry` | `(sload − dload) / (sload + dload + 1)` |
| `ttl_delta` | `sttl − dttl` |
| `loss_rate_src` | `sloss / max(spkts, 1)` |
| `loss_rate_dst` | `dloss / max(dpkts, 1)` |
| `pkts_per_sec` | `(spkts + dpkts) / dur` if `dur > 0`, else 0 |
| `log1p_sbytes` | `ln(1 + sbytes)` |
| `log1p_dbytes` | `ln(1 + dbytes)` |
| `log1p_dur` | `ln(1 + dur)` |
| `dur_bucket` | `<1 ms`=0, `<10 ms`=1, `<100 ms`=2, `<1 s`=3, else 4 |

Every gold feature is row-wise, so gold can be built incrementally.

## Model input — 54 features

At training and scoring time, `apply_frequency_encodings` adds
`proto_freq`, `service_freq`, `state_freq` from the frozen encoding table.
Model input = 38 raw + 13 derived + 3 frequency encodings = **54 features**
(numeric columns only; keys, labels, and strings excluded).

## Gold analytics

Recomputed from all of silver on every run of job 03.

| Column | Meaning |
|---|---|
| `attack_cat_norm` | category |
| `rows` | flow count |
| `avg_dur`, `avg_pkts_per_sec`, `avg_sbytes` | category averages |
| `pct_of_total` | share of all flows |

## Frequency encodings

| Column | Meaning |
|---|---|
| `column` | `proto`, `service`, or `state` |
| `value` | category value |
| `freq` | share of gold rows with that value at training time |

Values absent from the table (unseen at training time) encode as 0.0
([ADR-0013](adr/0013-freeze-frequency-encodings-at-training.md)).

## Predictions

| Column | Meaning |
|---|---|
| `flow_hash` | key, joins to gold features / silver |
| `pred_is_attack` | `pred_proba ≥ threshold` as int, where `threshold` comes from `model_meta.json` (tuned on validation; see [ADR-0017](adr/0017-validation-tuned-alert-threshold.md)) |
| `pred_proba` | binary model's probability of attack |
| `pred_attack_cat` | attack-family model's label: `normal` or one of the 9 families ([ADR-0018](adr/0018-separate-attack-family-model.md)). Meaningful on alerts; the two models can disagree |
| `model_version` | `--model_version` argument (default `v1`) |
| `scored_ts` | scoring time |

Exactly one row per gold flow (quality check 9). After retraining, run job 05
with `--full_refresh` so every flow is rescored with the new model.

## Model artifacts

Written together by job 04 (local `models/`, Databricks
`/Volumes/cyber_lakehouse/gold/artifacts/`); scoring refuses to run unless all
three exist plus the frequency-encodings table.

| File | Contents |
|---|---|
| `trained_model.pkl` (Databricks: `model.pkl`) | binary alerting model (joblib) |
| `attack_family_model.pkl` | 10-class attack-family model (joblib) |
| `model_meta.json` | `binary_model`, `threshold`, `target_recall`, `family_model`, `family_labels`, `features` (exact column order), `trained_at` |

## Model metrics

`reports/metrics.json` (local): dataset sizes and the 60/20/20 split; the
threshold policy; per binary model its tuned threshold, validation
precision/recall, and test metrics at the threshold and at 0.5; per family
model its validation macro-F1 and test accuracy, macro/weighted F1, per-class
precision/recall/F1/support, and confusion matrix; the selected models; feature
list and seed.

Databricks: `gold.model_metrics` (one row per binary model: threshold,
validation and test metrics, `selected`) and `gold.family_model_metrics` (one
row per family model: validation/test macro-F1, accuracy, `selected`); full
per-class results are logged to MLflow as `family_test_metrics.json`.
