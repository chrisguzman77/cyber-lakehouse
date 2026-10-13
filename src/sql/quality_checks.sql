-- Data quality checks across the lakehouse layers.
-- Every "violations" query must return 0; the summary queries are informational.

CREATE OR REPLACE TEMP VIEW bronze_traffic
USING delta
OPTIONS (path "/opt/project/data/bronze/traffic_delta");

CREATE OR REPLACE TEMP VIEW silver_traffic
USING delta
OPTIONS (path "/opt/project/data/silver/traffic_clean");

CREATE OR REPLACE TEMP VIEW gold_features
USING delta
OPTIONS (path "/opt/project/data/gold/features");

CREATE OR REPLACE TEMP VIEW gold_predictions
USING delta
OPTIONS (path "/opt/project/data/gold/predictions");

-- 1) Row counts per layer
SELECT 'bronze' AS layer, COUNT(*) AS rows FROM bronze_traffic
UNION ALL
SELECT 'silver', COUNT(*) FROM silver_traffic
UNION ALL
SELECT 'gold_features', COUNT(*) FROM gold_features
UNION ALL
SELECT 'gold_predictions', COUNT(*) FROM gold_predictions;

-- 2) Null labels in silver (must be 0)
SELECT COUNT(*) AS null_label_violations
FROM silver_traffic
WHERE label_int IS NULL;

-- 3) Negative durations in silver (must be 0)
SELECT COUNT(*) AS negative_dur_violations
FROM silver_traffic
WHERE dur < 0;

-- 4) Duplicate flow_hash in silver and gold (must be 0)
SELECT
  (SELECT COUNT(*) - COUNT(DISTINCT flow_hash) FROM silver_traffic)
    AS duplicate_flow_hash_violations,
  (SELECT COUNT(*) - COUNT(DISTINCT flow_hash) FROM gold_features)
    AS gold_duplicate_flow_hash_violations;

-- 5) is_attack outside {0,1} in gold features (must be 0)
SELECT COUNT(*) AS is_attack_domain_violations
FROM gold_features
WHERE is_attack NOT IN (0, 1) OR is_attack IS NULL;

-- 6) Prediction probabilities out of [0,1] or missing attack-family prediction (must be 0)
SELECT
  SUM(CASE WHEN pred_proba < 0 OR pred_proba > 1 OR pred_proba IS NULL THEN 1 ELSE 0 END)
    AS pred_proba_bounds_violations,
  SUM(CASE WHEN pred_attack_cat IS NULL THEN 1 ELSE 0 END) AS pred_attack_cat_null_violations
FROM gold_predictions;

-- 7) Attack-rate sanity: the raw dataset is ~87/13 benign/attack, but attack
-- flows (especially "generic") are heavily duplicated, so after flow-level
-- dedupe the labeled attack share sits around 4%.
SELECT
  ROUND(AVG(is_attack), 4) AS attack_share,
  CASE WHEN AVG(is_attack) BETWEEN 0.02 AND 0.25 THEN 'OK' ELSE 'SUSPICIOUS' END AS verdict
FROM gold_features;

-- 8) Null rates for key features in gold (informational; expect 0.0)
SELECT
  ROUND(AVG(CASE WHEN dur IS NULL THEN 1 ELSE 0 END), 4) AS dur_null_rate,
  ROUND(AVG(CASE WHEN sbytes IS NULL THEN 1 ELSE 0 END), 4) AS sbytes_null_rate,
  ROUND(AVG(CASE WHEN pkts_per_sec IS NULL THEN 1 ELSE 0 END), 4) AS pkts_per_sec_null_rate,
  ROUND(AVG(CASE WHEN proto IS NULL THEN 1 ELSE 0 END), 4) AS proto_null_rate
FROM gold_features;

-- 9) Every gold flow scored exactly once (must be 0; incremental MERGE is insert-only)
SELECT
  (SELECT COUNT(*) FROM gold_features) - (SELECT COUNT(*) FROM gold_predictions)
    AS unscored_flow_violations,
  (SELECT COUNT(*) - COUNT(DISTINCT flow_hash) FROM gold_predictions)
    AS duplicate_prediction_violations;
