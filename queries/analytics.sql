-- Analytics package for fraud.fact_transactions — run in Amazon Redshift (or Redshift Serverless).
-- Assumes fact rows are loaded with model scores, dollar amounts, timestamps, and booleans per schema.sql.

-- -----------------------------------------------------------------------------
-- Q1. How is fraud trending by calendar day (volume, flagged share, average risk
--     score, and financial exposure on flagged rows)? Use for executive daily briefs.
-- -----------------------------------------------------------------------------
SELECT
    CAST(transaction_timestamp AS DATE) AS txn_date,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(fraud_score) * 100, 2) AS avg_fraud_score_pct,
    SUM(CASE WHEN is_fraud_flagged THEN amount ELSE 0 END) AS total_fraud_usd
FROM fraud.fact_transactions
GROUP BY CAST(transaction_timestamp AS DATE)
ORDER BY txn_date DESC;


-- -----------------------------------------------------------------------------
-- Q2. Which merchant categories drive the most suspicious activity and how much
--     confirmed-fraud volume (USD) sits in each? Use to tune rules and monitoring.
-- -----------------------------------------------------------------------------
SELECT
    merchant_category,
    SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END) AS fraud_incident_count,
    SUM(CASE WHEN is_fraud_flagged THEN amount ELSE 0 END) AS total_fraud_usd
FROM fraud.fact_transactions
WHERE merchant_category IS NOT NULL
GROUP BY merchant_category
ORDER BY fraud_incident_count DESC;


-- -----------------------------------------------------------------------------
-- Q3. Does cross-border traffic carry higher fraud rates than domestic? Compares
--     overall volume to flagged counts by is_international for policy and limits.
-- -----------------------------------------------------------------------------
SELECT
    is_international,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END) AS flagged_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END)::DECIMAL(18, 4)
        / NULLIF(COUNT(*), 0),
        2
    ) AS fraud_rate_pct
FROM fraud.fact_transactions
GROUP BY is_international
ORDER BY is_international DESC;


-- -----------------------------------------------------------------------------
-- Q4. When during the day do fraud attempts cluster? Supports staffing, batch job
--     windows, and anomaly detection on hourly attack patterns (server local hour).
-- -----------------------------------------------------------------------------
SELECT
    transaction_hour,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud_flagged THEN 1 ELSE 0 END)::DECIMAL(18, 4)
        / NULLIF(COUNT(*), 0),
        2
    ) AS fraud_rate_pct
FROM fraud.fact_transactions
WHERE transaction_hour BETWEEN 0 AND 23
GROUP BY transaction_hour
ORDER BY transaction_hour;


-- -----------------------------------------------------------------------------
-- Q5. Which high-risk, high-ticket rows need immediate investigator review? Surfaces
--     full detail for transactions with strong model scores plus material dollars.
-- -----------------------------------------------------------------------------
SELECT *
FROM fraud.fact_transactions
WHERE fraud_score > 0.75
  AND amount > 500
ORDER BY fraud_score DESC;
