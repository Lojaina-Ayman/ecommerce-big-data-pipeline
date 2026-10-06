package com.ecom

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

final case class LedgerRow(
    query_name: String, batch_id: Long, signature: String,
    total_rows: Long, valid_rows: Long, quarantined_rows: Long, duplicates_removed: Long,
    processed_at: java.sql.Timestamp)

class ClickHouseSink(db: String, queryName: String) {

  private def table(name: String): String = s"clickhouse.$db.$name"

  private def alignTo(spark: SparkSession, df: DataFrame, tableName: String): DataFrame = {
    val target: StructType = spark.table(table(tableName)).schema
    val cols = target.fields.map { f =>
      require(df.columns.contains(f.name), s"Column '${f.name}' of $tableName is missing from the DataFrame")
      col(f.name).cast(f.dataType).as(f.name)
    }
    df.select(cols: _*)
  }

  private def signature(scored: DataFrame): String =
    scored.groupBy("kafka_partition")
      .agg(min("kafka_offset").as("lo"), max("kafka_offset").as("hi"))
      .orderBy("kafka_partition").collect()
      .map(r => s"${r.getInt(0)}:${r.getLong(1)}-${r.getLong(2)}").mkString(";")

  private def alreadyProcessed(spark: SparkSession, sig: String): Boolean =
    spark.table(table("batch_ledger"))
      .filter(col("query_name") === queryName && col("signature") === sig)
      .limit(1).count() > 0

  def process(scored: DataFrame, batchId: Long): Unit = {
    val spark = scored.sparkSession
    import spark.implicits._

    scored.persist()
    try {
      val total = scored.count()
      if (total > 0) {
        val sig = signature(scored)
        if (alreadyProcessed(spark, sig)) {
          println(s"[batch $batchId] SKIPPED: offsets $sig are already in batch_ledger (replay after restart)")
        } else {
          val validAll   = Transformations.validRows(scored)
          val valid      = validAll.dropDuplicates(Transformations.IdentityKeys)
          val bad        = Transformations.quarantineRows(scored)

          val validCount = valid.count()
          val dupRemoved = validAll.count() - validCount
          val badCount   = bad.count()

          val validOut = valid
            .withColumn("category_id",   coalesce(col("category_id"), lit(0L)))
            .withColumn("category_code", coalesce(col("category_code"), lit("")))
            .withColumn("user_session",  coalesce(col("user_session"), lit("")))
            .withColumn("price",         coalesce(col("price"), lit(0.0)))
            .withColumn("ingested_at",   current_timestamp())

          if (validCount > 0)
            alignTo(spark, validOut, "ecommerce_events").writeTo(table("ecommerce_events_buffer")).append()
          if (badCount > 0)
            alignTo(spark, bad, "events_quarantine").writeTo(table("events_quarantine_buffer")).append()

          val row = LedgerRow(queryName, batchId, sig, total, validCount, badCount, dupRemoved,
            new java.sql.Timestamp(System.currentTimeMillis()))
          alignTo(spark, Seq(row).toDF(), "batch_ledger").writeTo(table("batch_ledger")).append()

          println(s"[batch $batchId] written: valid=$validCount quarantined=$badCount " +
            s"duplicates_removed=$dupRemoved total=$total offsets=$sig")
        }
      }
    } finally {
      scored.unpersist()
    }
  }
}
