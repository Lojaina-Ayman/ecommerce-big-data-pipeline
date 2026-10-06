#!/bin/sh
exec /opt/spark/bin/spark-sql \
  --master local[1] \
  --packages com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.9.0,com.clickhouse:clickhouse-jdbc:0.9.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.driver.memory=512m \
  --conf spark.sql.catalog.clickhouse=com.clickhouse.spark.ClickHouseCatalog \
  --conf spark.sql.catalog.clickhouse.host=clickhouse \
  --conf spark.sql.catalog.clickhouse.protocol=http \
  --conf spark.sql.catalog.clickhouse.http_port=8123 \
  --conf spark.sql.catalog.clickhouse.user="$CLICKHOUSE_USER" \
  --conf spark.sql.catalog.clickhouse.password="$CLICKHOUSE_PASSWORD" \
  --conf spark.sql.catalog.clickhouse.database="$CLICKHOUSE_DB" \
  "$@"
