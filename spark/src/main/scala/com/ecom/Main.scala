package com.ecom

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}

class ProgressListener extends StreamingQueryListener {
  override def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit =
    println(s"[query started] name=${e.name}")

  override def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = {
    val p = e.progress
    println(s"[progress] ${p.name} batch=${p.batchId} rows=${p.numInputRows} " +
      s"in/s=${math.round(p.inputRowsPerSecond)} processed/s=${math.round(p.processedRowsPerSecond)} " +
      s"trigger_ms=${p.durationMs.get("triggerExecution")}")
  }

  override def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit =
    println(s"[query terminated] id=${e.id} error=${e.exception.getOrElse("none")}")
}

object Main {

  def main(args: Array[String]): Unit = {
    val cfg = Config.fromEnv()

    val base = SparkSession.builder()
      .appName(s"ecommerce-streaming-${cfg.mode}-${cfg.sink}")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "4")

    val builder =
      if (cfg.sink == "clickhouse")
        base
          .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
          .config("spark.sql.catalog.clickhouse.host", cfg.ch.host)
          .config("spark.sql.catalog.clickhouse.protocol", "http")
          .config("spark.sql.catalog.clickhouse.http_port", cfg.ch.httpPort)
          .config("spark.sql.catalog.clickhouse.user", cfg.ch.user)
          .config("spark.sql.catalog.clickhouse.password", cfg.ch.password)
          .config("spark.sql.catalog.clickhouse.database", cfg.ch.db)
          .config("spark.clickhouse.write.format", sys.env.getOrElse("CH_WRITE_FORMAT", "json"))
          .config("spark.clickhouse.write.batchSize", cfg.chWriteBatchSize)
          .config("spark.clickhouse.write.repartitionByPartition", cfg.chRepartitionByPartition)
      else base

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(new ProgressListener)

        val baseReader = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrap)
      .option("subscribe", cfg.topic)
      .option("startingOffsets", cfg.startingOffsets)
      .option("maxOffsetsPerTrigger", cfg.maxOffsetsPerTrigger)
      .option("failOnDataLoss", "true")
    val reader = sys.env.get("KAFKA_MIN_PARTITIONS").map(_.trim).filter(_.nonEmpty)
      .fold(baseReader)(v => baseReader.option("minPartitions", v))
    val kafka = reader.load()

    val scored  = Transformations.score(kafka)
    val trigger = if (cfg.triggerSeconds == 0) Trigger.AvailableNow() else Trigger.ProcessingTime(s"${cfg.triggerSeconds} seconds")

    val query = cfg.mode match {
      case "clean"  => cleanQuery(scored, cfg, trigger)
      case "window" => windowQuery(scored, cfg, trigger)
      case other    => throw new IllegalArgumentException(s"Unknown MODE '$other' (use clean or window)")
    }
    query.awaitTermination()
  }

  private def cleanQuery(scored: DataFrame, cfg: Config, trigger: Trigger): StreamingQuery = {
    val writer: (DataFrame, Long) => Unit = cfg.sink match {
      case "log"  => Sinks.logSink
      case "noop" => Sinks.noopSink
      case "clickhouse" =>
        val sink = new ClickHouseSink(cfg.ch.db, "ecommerce_clean")
        (df: DataFrame, id: Long) => sink.process(df, id)
      case other => throw new IllegalArgumentException(s"Unknown SINK '$other' (use log, noop, or clickhouse)")
    }
    scored.writeStream
      .queryName("ecommerce_clean")
      .option("checkpointLocation", cfg.checkpointDir)
      .trigger(trigger)
      .foreachBatch(writer)
      .start()
  }

  private def windowQuery(scored: DataFrame, cfg: Config, trigger: Trigger): StreamingQuery = {
    val valid = scored.filter(col("reject_reason").isNull)
    val hourly = valid
      .withWatermark("event_time", "10 minutes")
      .groupBy(window(col("event_time"), "1 hour"), col("event_type"))
      .agg(
        count(lit(1)).as("events"),
        sum(when(col("event_type") === "purchase", col("price")).otherwise(lit(0.0))).as("revenue"))

    hourly.writeStream
      .queryName("ecommerce_hourly_funnel")
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "50")
      .option("checkpointLocation", cfg.checkpointDir)
      .trigger(trigger)
      .start()
  }
}
