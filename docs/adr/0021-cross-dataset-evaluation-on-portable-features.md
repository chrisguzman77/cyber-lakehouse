# ADR-0021: Cross-dataset evaluation on a portable feature set

- **Status:** Accepted
- **Date:** 2026-10-14

## Context

Every reported score comes from a test split of UNSW-NB15, so it measures how
well the model separates UNSW's attacks from UNSW's background traffic. It says
nothing about another network. Some UNSW features (TTLs in particular) are
known to be unusually predictive on this dataset, which suggests the within-dataset
numbers are optimistic. CIC-IDS2017 is the obvious second benchmark: it comes from a different
lab, different attacks (DoS/DDoS, brute force, web attacks, botnet, port
scan), and a different flow exporter (CICFlowMeter instead of Argus/Bro).

The two datasets share no schema. Most UNSW features have no CIC counterpart
(`ct_*` connection counters, service, state), and some same-named fields use
different units. UNSW stores the TCP window as a scaled value (mostly 255),
while CIC stores raw bytes (e.g. 29200).

## Decision

- **Direction:** train on UNSW and test on CIC, which is the question the
  project's reported numbers raise. Report two baselines next to the transfer
  result:
  - **Feature-loss baseline:** UNSW trained and tested on the portable
    features.
  - **Ceiling:** CIC trained and tested on the portable features.

  These separate "the features are too weak" from "the model doesn't
  generalize".
- **Portable features:** only the 7 measurements with the same meaning in both
  datasets: `dur`, `sbytes`, `dbytes`, `spkts`, `dpkts`, `sintpkt`,
  `dintpkt`. CIC values are converted into UNSW units:
  - duration µs → s, and mean inter-arrival time µs → ms;
  - bytes = payload + header length, because UNSW byte counts include headers;
  - negative CICFlowMeter header lengths are clipped to 0;
  - rows with non-finite values or a negative duration are dropped.

  12 derived features are computed from these 7 with the gold-layer formulas,
  19 in total. A Spark test checks that the derived features match
  `add_derived_features`.
- **Excluded:** TTLs, protocol/service/state, `ct_*`, and TCP window sizes:
  no counterpart, or a counterpart in different units.
- **Same protocol as training:** 60/20/20 split stratified by label,
  threshold = highest validation precision at ≥95% recall, three model
  families with selection on validation. The transfer is scored at the
  **UNSW-chosen threshold**, as a deployed model would be. The CIC-optimal
  ("oracle") threshold is also reported, as a reference only.
- **Data handling:**
  - CIC is deduplicated on the core measurements + label (2.83M → 1.98M rows;
    port scans collapse heavily), then sampled to 500k with the same fraction
    per label.
  - UNSW gold is sampled to 500k.
- **Binary only:** the two datasets' attack families don't map onto each other
  (e.g. CIC has no fuzzers or shellcode; UNSW has no brute force), so the
  family model is not tested. A per-CIC-attack-type detection rate is reported
  instead.
- **Standalone local job** (`src/jobs/evaluate_cross_dataset.py`), not part of
  either Workflow. It is a one-off evaluation, not a recurring pipeline step.

## Alternatives considered

- **Map more features (TCP windows, TTLs, protocol).** Rejected: these fields
  have different semantics or units, and mapping them would make the test
  measure the mapping rather than the model.
- **Score the production 54-feature model on CIC with imputed values** for the
  missing columns. Rejected: imputed values would dominate the result.
- **Also train on CIC and test on UNSW.** Not done: it adds a second table
  without changing the conclusion. The job can be extended if needed.
- **Combine both datasets for training.** A different question (domain
  generalization) and out of scope.
- **Run on Databricks.** Unnecessary for a one-off evaluation. CIC would
  also need a new Volume and a Workflow.

## Consequences

- Result (2026-10-14):

  | Run | Test F1 | Test ROC-AUC |
  |---|---|---|
  | UNSW model on portable features, UNSW test | 0.861 | — |
  | Same model on CIC (UNSW threshold) | **0.028** | **0.470** |
  | CIC-trained ceiling, CIC test | 0.977 | — |

  - On CIC, the UNSW model detects 3.2% of attacks and flags 23.6% of benign
    flows.
  - Even the CIC-optimal threshold only reaches 0.19 precision at 95% recall,
    against a 16.5% attack share.
  - The features are sufficient (the ceiling proves it). What doesn't carry
    over is what the model learned from UNSW.
- Diagnosis from per-class medians:
  - CIC benign flows (median 2 packets, 140 B) look like UNSW `generic`
    attacks (2 packets, 114 B); UNSW benign is 16–18 packets, ~2 KB.
  - CIC DoS flows are long and slow (median 60–100 s), unlike UNSW attacks.
- The README states plainly that the 0.89 F1 is a within-dataset number and
  that deployment on a new network needs retraining on local traffic.
- Precision is not comparable across the two test sets, because attack
  prevalence differs (~4% UNSW vs 16.5% CIC). ROC-AUC and average precision
  are prevalence-independent and are the fair comparison.
- CIC data (~850 MB) is fetched on demand by `scripts/fetch_cic.sh` and is
  gitignored. Results are committed in `reports/cross_dataset.json`.
