-- 1) Attack counts by category (from gold.analytics)
SELECT * FROM delta.`data/gold/analytics`
ORDER BY rows DESC;

-- 2) Prediction distribution
SELECT
    pred_is_attack,
    COUNT(*) AS n
    FROM delta.`data/gold/predictions`
    GROUP BY pred_is_attack
    ORDER BY n DESC;

-- 3) Top "most suspicious" flows (highest probability)
SELECT
    flow_hash,
    pred_proba,
    model_version,
    scored_ts
FROM delta.`data/gold/predictions`
ORDER BY pred_proba DESC
LIMIT 20;