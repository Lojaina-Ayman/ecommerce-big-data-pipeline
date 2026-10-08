package com.ecom

final case class ClickHouseConf(host: String, httpPort: String, user: String, password: String, db: String)

final case class Config(
    bootstrap: String,
    topic: String,
    mode: String,                 // clean | window
    sink: String,                 // log | noop | clickhouse
    checkpointDir: String,
    maxOffsetsPerTrigger: Long,
    triggerSeconds: Int,
    startingOffsets: String,
    chWriteBatchSize: String,
    chRepartitionByPartition: String,
    ch: ClickHouseConf
)

object Config {
  private def env(key: String, default: String): String =
    sys.env.get(key).map(_.trim).filter(_.nonEmpty).getOrElse(default)

  def fromEnv(): Config = {
    val mode = env("MODE", "clean")
    val sink = env("SINK", "log")
    Config(
      bootstrap                = env("KAFKA_BOOTSTRAP", "kafka:9092"),
      topic                    = env("KAFKA_TOPIC", "ecommerce_events_stream"),
      mode                     = mode,
      sink                     = sink,
      checkpointDir            = env("CHECKPOINT_DIR", s"/opt/spark-checkpoints/${mode}_${sink}_v1"),
      maxOffsetsPerTrigger     = env("MAX_OFFSETS_PER_TRIGGER", "20000").toLong,
      triggerSeconds           = env("TRIGGER_SECONDS", "10").toInt,
      startingOffsets          = env("STARTING_OFFSETS", "earliest"),
      chWriteBatchSize         = env("CH_WRITE_BATCH_SIZE", "10000"),
      chRepartitionByPartition = env("CH_REPARTITION_BY_PARTITION", "true"),
      ch = ClickHouseConf(
        host     = env("CLICKHOUSE_HOST", "clickhouse"),
        httpPort = env("CLICKHOUSE_HTTP_PORT", "8123"),
        user     = env("CLICKHOUSE_USER", "default"),
        password = env("CLICKHOUSE_PASSWORD", ""),
        db       = env("CLICKHOUSE_DB", "ecommerce"))
    )
  }
}