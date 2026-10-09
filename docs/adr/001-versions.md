# ADR means Architecture Decision Record
# ADR 001: Pinned versions for Spark, Delta Lake, Hadoop-AWS and Kafka connector

- **Status:** Accepted
- **Date:** 2026-10-08

## Context

Spark, Delta Lake, hadoop-aws and the Kafka connector must be mutually
compatible (same Scala version, same Spark line, hadoop-aws matching the
Hadoop version bundled with Spark). Mismatches cause runtime errors such as
NoSuchMethodError when reading or writing to S3.

## Decision

| Component | Version |
|---|---|
| Java | 17 |
| Python | <fill in> |
| Scala | 2.12 |
| Spark / PySpark | 3.5.1 |
| Delta Lake | 3.2.0 (io.delta:delta-spark_2.12) |
| hadoop-aws | 3.3.4 |
| aws-java-sdk-bundle | 1.12.262 |
| Kafka connector | org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 |
| Redpanda | <pin a specific image tag> |

## Alternatives considered

- **Spark 4.x + Delta 4.x:** newer, but less documentation and fewer
  community examples for the Kafka + S3 combination.
- **Floating versions (latest):** rejected, because it breaks reproducibility.

## Consequences

- All versions are pinned in `pyproject.toml`, `docker-compose.yml` and the
  Spark `--packages` configuration.
- Upgrading any one component requires checking the others and updating this ADR.

## How it was validated

`make smoke-test` writes and reads a Delta table on S3 with these versions.
