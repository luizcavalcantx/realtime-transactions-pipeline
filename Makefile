PACKAGES = io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

.PHONY: up down logs smoke-test

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

smoke-test:
	docker compose exec spark /opt/spark/bin/spark-submit \
		--packages $(PACKAGES) \
		--conf spark.jars.ivy=/tmp/ivy \
		/opt/app/scripts/smoke_test.py
