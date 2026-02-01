-- Register Delta locations as temp views
CREATE OR REPLACE TEMP VIEW gold_analytics
USING delta
OPTIONS (path "/opt/project/data/gold/analytics");

CREATE OR REPLACE TEMP VIEW gold_predictions
USING delta
OPTIONS (path "/opt/project/data/gold/predictions");

-- 1) Attack counts by category (from gold.analytics)
SELECT *
FROM gold_analytics
ORDER BY rows DESC;

-- 2) Prediction distribution
SELECT
  pred_is_attack,
  COUNT(*) AS n
FROM gold_predictions
GROUP BY pred_is_attack
ORDER BY n DESC;

-- 3) Top "most suspicious" flows (highest probability)
SELECT
  flow_hash,
  pred_proba,
  model_version,
  scored_ts
FROM gold_predictions
ORDER BY pred_proba DESC
LIMIT 20;
