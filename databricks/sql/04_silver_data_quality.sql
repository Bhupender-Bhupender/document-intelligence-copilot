-- Phase 7: persistent Silver data-quality monitoring

USE CATALOG docintel_dev;

CREATE SCHEMA IF NOT EXISTS monitoring
COMMENT 'Pipeline execution and operational quality metrics';


CREATE TABLE IF NOT EXISTS docintel_dev.monitoring.silver_quality_runs
(
    run_id             STRING,
    started_at         TIMESTAMP,
    completed_at       TIMESTAMP,

    total_checks       INT,
    passed_checks      INT,
    failed_checks      INT,
    critical_failures  INT,

    run_status         STRING,
    notes              STRING
)
USING DELTA
COMMENT 'Run-level history for canonical Silver quality validation';


CREATE TABLE IF NOT EXISTS docintel_dev.monitoring.silver_quality_results
(
    run_id             STRING,
    checked_at         TIMESTAMP,

    check_name         STRING,
    entity             STRING,
    severity           STRING,

    passed             BOOLEAN,
    violation_count    BIGINT,

    expected_value     STRING,
    observed_value     STRING
)
USING DELTA
COMMENT 'Individual Silver data-quality check results';
