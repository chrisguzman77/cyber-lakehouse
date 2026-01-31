-- Row counts by layer
SELECT COUNT(*) AS bronze_rows FROM delta.`data/bronze/traffic_delta`;
SELECT COUNT(*) AS silver_rows FROM delta.`data/silver/traffic_clean`;
SELECT COUNT(*) AS gold_features_rows FROM delta.`data/gold.features`;

-- Null checks (Silver)
SELECT COUNT(*) AS null_label
FROM delta.`data/silver/traffic_clean`
WHERE label_int IS NULL;

-- Basic sanity: duration should be non-negative
SELECT COUNT(*) AS negative_dur
FROM delta.`data/silver/traffic_clean`
WHERE dur < 0;

-- Schema check example (is_attack must exist in Gold)
DESCRIBE TABLE delta.`data/gold/features`;