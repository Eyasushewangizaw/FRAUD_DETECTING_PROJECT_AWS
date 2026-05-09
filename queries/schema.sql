-- DDL for core fraud mart tables on Amazon Redshift (fact + merchant dimension aligned to pipeline curated fields).

CREATE SCHEMA fraud;

CREATE TABLE fraud.fact_transactions (
    transaction_id          VARCHAR(50) NOT NULL,
    card_number_hash        VARCHAR(64),
    merchant_id             VARCHAR(50) NOT NULL,
    merchant_category       VARCHAR(50),
    amount                  DECIMAL(10, 2),
    transaction_timestamp   TIMESTAMP,
    transaction_country     VARCHAR(3),
    card_present            BOOLEAN,
    is_international        BOOLEAN,
    transaction_hour        INTEGER,
    is_weekend              BOOLEAN,
    fraud_score             DECIMAL(5, 4),
    is_fraud_flagged        BOOLEAN,
    processing_timestamp    TIMESTAMP,
    PRIMARY KEY (transaction_id)
)
-- DISTKEY shards rows by merchant so heavy merchant-level joins/aggregates with dim_merchants stay co-located instead of reshuffling wide fact rows across nodes.
DISTSTYLE KEY
DISTKEY (merchant_id)
-- SORTKEY orders storage on transaction time so time-bounded dashboards, alerts, and “last N days” scans prune blocks via zone maps.
SORTKEY (transaction_timestamp);

CREATE TABLE fraud.dim_merchants (
    merchant_id       VARCHAR(50) NOT NULL PRIMARY KEY,
    merchant_name     VARCHAR(256),
    merchant_category VARCHAR(50),
    risk_level        VARCHAR(20),
    country           VARCHAR(3)
);
