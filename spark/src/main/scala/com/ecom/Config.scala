package com.ecom

final case class Config(
    bootstrap: String,
    topic: String,
    mode: String,
    checkpointDir: String,
    maxOffsetsPerTrigger: Long,
    triggerSeconds: Int,
    startingOffsets: String,
    sink: String,
    clickhouseUrl: String,
    clickhouseUser: String,
    clickhousePassword: String
)

object Config {
  private def env(key: String, default: String): String =
    sys.env.get(key).map(_.trim).filter(_.nonEmpty).getOrElse(default)

  def fromEnv(): Config = {
    val mode = env("MODE", "clean")
    Config(
      bootstrap            = env("KAFKA_BOOTSTRAP", "kafka:9092"),
      topic                = env("KAFKA_TOPIC", "ecommerce_events_stream"),
      mode                 = mode,
      checkpointDir        = env("CHECKPOINT_DIR", s"/opt/spark-checkpoints/${mode}_v1"),
      maxOffsetsPerTrigger = env("MAX_OFFSETS_PER_TRIGGER", "20000").toLong,
      triggerSeconds       = env("TRIGGER_SECONDS", "10").toInt,
      startingOffsets      = env("STARTING_OFFSETS", "earliest"),
      sink                 = env("SINK", "log"),
      clickhouseUrl        = env("CLICKHOUSE_URL", "jdbc:ch://clickhouse:8123/ecommerce"),
      clickhouseUser       = env("CLICKHOUSE_USER", "default_admin"),
      clickhousePassword   = env("CLICKHOUSE_PASSWORD", "change_me_clickhouse")
    )
  }
}
