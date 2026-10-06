package com.ecom

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp, Date => SqlDate}
import java.util.Properties

object Sinks {

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

  def clickhouseSink(cfg: Config)(scored: DataFrame, batchId: Long): Unit = {
    scored.persist()
    try {
      val valid = Transformations.validRows(scored)
      val bad   = Transformations.quarantineRows(scored)

      val total      = scored.count()
      val validCount = valid.count()
      val badCount   = bad.count()

      println(s"[batch $batchId] SINK=clickhouse total=$total valid=$validCount quarantined=$badCount")

      val chUrl  = cfg.clickhouseUrl
      val chUser = cfg.clickhouseUser
      val chPass = cfg.clickhousePassword
      val topic  = cfg.topic

      def buildProperties(): Properties = {
        val props = new Properties()
        props.setProperty("user", chUser)
        props.setProperty("password", chPass)
        props.setProperty("compress", "0")
        props.setProperty("decompress", "0")
        props.setProperty("custom_settings", "enable_http_compression=0")
        props
      }

      if (validCount > 0) {
        valid.rdd.foreachPartition { (partition: Iterator[Row]) =>
          if (partition.hasNext) {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
            val conn = DriverManager.getConnection(chUrl, buildProperties())

            val sql =
              """
                |INSERT INTO ecommerce.clean_events (
                |  event_time, event_date, event_hour, hour_of_day, day_of_week,
                |  event_type, product_id, category_id, category_code,
                |  primary_category, sub_category, category_depth,
                |  brand, price, user_id, user_session, dq_flags,
                |  kafka_partition, kafka_offset
                |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              """.stripMargin

            val stmt = conn.prepareStatement(sql)
            try {
              var count = 0
              while (partition.hasNext) {
                val row = partition.next()
                stmt.setTimestamp(1, Option(row.getAs[Timestamp]("event_time")).orNull)
                stmt.setDate(2, Option(row.getAs[SqlDate]("event_date")).orNull)
                stmt.setTimestamp(3, Option(row.getAs[Timestamp]("event_hour")).orNull)
                stmt.setInt(4, row.getAs[Number]("hour_of_day").intValue())
                stmt.setInt(5, row.getAs[Number]("day_of_week").intValue())
                stmt.setString(6, row.getAs[String]("event_type"))
                stmt.setLong(7, row.getAs[Number]("product_id").longValue())
                stmt.setLong(8, Option(row.getAs[Number]("category_id")).map(_.longValue()).getOrElse(0L))
                stmt.setString(9, Option(row.getAs[String]("category_code")).getOrElse(""))
                stmt.setString(10, Option(row.getAs[String]("primary_category")).getOrElse(""))
                stmt.setString(11, Option(row.getAs[String]("sub_category")).getOrElse(""))
                stmt.setInt(12, Option(row.getAs[Number]("category_depth")).map(_.intValue()).getOrElse(0))
                stmt.setString(13, Option(row.getAs[String]("brand")).getOrElse(""))
                stmt.setBigDecimal(14, Option(row.getAs[Number]("price")).map(p => new java.math.BigDecimal(p.toString)).orNull)
                stmt.setLong(15, row.getAs[Number]("user_id").longValue())
                stmt.setString(16, Option(row.getAs[String]("user_session")).getOrElse("00000000-0000-0000-0000-000000000000"))

                // Safely extract Spark Array without casting to immutable Seq
                val flagsLiteral = if (row.isNullAt(row.fieldIndex("dq_flags"))) {
                  "[]"
                } else {
                  import scala.jdk.CollectionConverters._
                  val flagsList = row.getList[String](row.fieldIndex("dq_flags")).asScala
                  flagsList.map(f => "'" + f.replace("'", "''") + "'").mkString("[", ",", "]")
                }
                stmt.setString(17, flagsLiteral)

                stmt.setInt(18, row.getAs[Number]("kafka_partition").intValue())
                stmt.setLong(19, row.getAs[Number]("kafka_offset").longValue())

                stmt.addBatch()
                count += 1
                if (count % 1000 == 0) {
                  stmt.executeBatch()
                }
              }
              stmt.executeBatch()
            } finally {
              stmt.close()
              conn.close()
            }
          }
          ()
        }
      }

      if (badCount > 0) {
        bad.rdd.foreachPartition { (partition: Iterator[Row]) =>
          if (partition.hasNext) {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
            val conn = DriverManager.getConnection(chUrl, buildProperties())

            val sql =
              """
                |INSERT INTO ecommerce.quarantine_events (
                |  reject_reason, raw_value, kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
                |) VALUES (?, ?, ?, ?, ?, ?)
              """.stripMargin

            val stmt = conn.prepareStatement(sql)
            try {
              var count = 0
              while (partition.hasNext) {
                val row = partition.next()
                stmt.setString(1, row.getAs[String]("reject_reason"))
                stmt.setString(2, row.getAs[String]("raw_value"))
                stmt.setString(3, topic)
                stmt.setInt(4, row.getAs[Number]("kafka_partition").intValue())
                stmt.setLong(5, row.getAs[Number]("kafka_offset").longValue())
                stmt.setTimestamp(6, Option(row.getAs[Timestamp]("kafka_timestamp")).orNull)

                stmt.addBatch()
                count += 1
                if (count % 1000 == 0) {
                  stmt.executeBatch()
                }
              }
              stmt.executeBatch()
            } finally {
              stmt.close()
              conn.close()
            }
          }
          ()
        }
      }
    } finally {
      scored.unpersist()
    }
  }
}
