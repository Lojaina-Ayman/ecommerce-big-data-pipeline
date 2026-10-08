package com.ecom

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Sinks {

  /** Console log sink for validating micro-batches. */
  def logSink(scored: DataFrame, batchId: Long): Unit = {
    scored.persist()
    try {
      val valid = Transformations.validRows(scored)
      val bad   = Transformations.quarantineRows(scored)

      val total         = scored.count()
      val validCount    = valid.count()
      val badCount      = bad.count()
      val distinctValid = valid.dropDuplicates(Transformations.IdentityKeys).count()

      println(s"[batch $batchId] total=$total valid=$validCount quarantined=$badCount " +
        s"duplicates_in_batch=${validCount - distinctValid} " +
        s"check=${if (total == validCount + badCount) "OK" else "MISMATCH"}")

      if (badCount > 0) {
        println(s"[batch $batchId] reject reasons:")
        bad.groupBy("reject_reason").count().orderBy(desc("count")).collect()
          .foreach(r => println(s"    ${r.getString(0)}: ${r.getLong(1)}"))
        bad.select("reject_reason", "raw_value").show(5, truncate = false)
      }
      if (batchId == 0L) valid.show(5, truncate = false)
      valid.filter(size(col("dq_flags")) > 0)
        .select("event_type", "product_id", "user_id", "dq_flags").show(3, truncate = false)
    } finally {
      scored.unpersist()
    }
  }

  /** Benchmark sink: full computation, no output. Measures Spark alone. */
  def noopSink(scored: DataFrame, batchId: Long): Unit = {
    scored.persist()
    try {
      Transformations.validRows(scored)
        .dropDuplicates(Transformations.IdentityKeys)
        .write.format("noop").mode("append").save()
      Transformations.quarantineRows(scored)
        .write.format("noop").mode("append").save()
    } finally {
      scored.unpersist()
    }
  }
}
