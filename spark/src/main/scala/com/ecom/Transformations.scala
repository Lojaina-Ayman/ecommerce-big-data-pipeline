package com.ecom

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Transformations {

  val ValidEventTypes: Seq[String] = Seq("view", "cart", "purchase")

  val IdentityKeys: Seq[String] =
    Seq("event_time", "event_type", "product_id", "user_id", "user_session")

  private val RawColumns = Seq(
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session")

  private val CorruptCol = "_corrupt_record"

  val rawSchema: StructType = StructType(
    (RawColumns :+ CorruptCol).map(n => StructField(n, StringType, nullable = true)))

  private def clean(c: Column): Column =
    when(length(trim(c)) === 0, lit(null).cast(StringType)).otherwise(trim(c))

  def parse(kafka: DataFrame): DataFrame = {
    val opts = Map("mode" -> "PERMISSIVE", "columnNameOfCorruptRecord" -> CorruptCol)
    val withJson = kafka
      .select(
        col("partition").as("kafka_partition"),
        col("offset").as("kafka_offset"),
        col("timestamp").as("kafka_timestamp"),
        col("value").cast(StringType).as("raw_value"))
      .withColumn("j", from_json(col("raw_value"), rawSchema, opts))

    val allNull = RawColumns.map(n => col(s"j.$n").isNull).reduce(_ && _)

    withJson
      .select(
        col("kafka_partition"), col("kafka_offset"), col("kafka_timestamp"), col("raw_value"),
        (col(s"j.$CorruptCol").isNotNull || allNull).as("json_malformed"),
        col("j.*"))
      .drop(CorruptCol)
  }

  def typed(parsed: DataFrame): DataFrame = {
    val base = parsed.select(
      col("kafka_partition"), col("kafka_offset"), col("kafka_timestamp"),
      col("raw_value"), col("json_malformed"),
      clean(col("event_time")).as("event_time_s"),
      lower(clean(col("event_type"))).as("event_type"),
      clean(col("product_id")).as("product_id_s"),
      clean(col("category_id")).as("category_id_s"),
      lower(clean(col("category_code"))).as("category_code"),
      lower(clean(col("brand"))).as("brand_s"),
      clean(col("price")).as("price_s"),
      clean(col("user_id")).as("user_id_s"),
      clean(col("user_session")).as("user_session"))

    base
      .withColumn("event_time",
        try_to_timestamp(regexp_replace(col("event_time_s"), " UTC$", ""), lit("yyyy-MM-dd HH:mm:ss")))
      .withColumn("product_id",  expr("try_cast(product_id_s AS BIGINT)"))
      .withColumn("category_id", expr("try_cast(category_id_s AS BIGINT)"))
      .withColumn("price",       expr("try_cast(price_s AS DOUBLE)"))
      .withColumn("user_id",     expr("try_cast(user_id_s AS BIGINT)"))
  }

  def withQuality(t: DataFrame): DataFrame = {
    val et = col("event_type")
    val p  = col("price")

    val reason: Column =
      when(col("json_malformed"), lit("malformed_json"))
        .when(col("event_time").isNull, lit("invalid_timestamp"))
        .when(et.isNull || !et.isin(ValidEventTypes: _*), lit("invalid_event_type"))
        .when(col("product_id").isNull && col("product_id_s").isNull, lit("missing_product_id"))
        .when(col("product_id").isNull, lit("invalid_product_id"))
        .when(col("user_id").isNull && col("user_id_s").isNull, lit("missing_user_id"))
        .when(col("user_id").isNull, lit("invalid_user_id"))
        .when(col("category_id").isNull && col("category_id_s").isNotNull, lit("invalid_category_id"))
        .when(p.isNull && col("price_s").isNotNull, lit("invalid_price"))
        .when(isnan(p) || p.isin(Double.PositiveInfinity, Double.NegativeInfinity), lit("invalid_price"))
        .when(p < 0, lit("negative_price"))
        .when(et === "purchase" && p.isNull, lit("missing_price_on_purchase"))

    val flags: Column = array_compact(array(
      when(col("user_session").isNull, lit("missing_user_session")),
      when(col("brand_s").isNull,      lit("missing_brand")),
      when(col("category_code").isNull, lit("missing_category_code")),
      when(p.isNull,                   lit("missing_price")),
      when(p === 0.0,                  lit("zero_price"))))

    t.withColumn("reject_reason", reason).withColumn("dq_flags", flags)
  }

  def withFeatures(df: DataFrame): DataFrame = {
    val parts = split(col("category_code"), "\\.")
    df
      .withColumn("event_date",  to_date(col("event_time")))
      .withColumn("event_hour",  date_trunc("hour", col("event_time")))
      .withColumn("hour_of_day", hour(col("event_time")))
      .withColumn("day_of_week", ((dayofweek(col("event_date")) + 5) % 7) + 1)
      .withColumn("category_depth", when(col("category_code").isNull, lit(0)).otherwise(size(parts)))
      .withColumn("primary_category", coalesce(try_element_at(parts, lit(1)), lit("unknown")))
      .withColumn("sub_category",     coalesce(try_element_at(parts, lit(2)), lit("unknown")))
      .withColumn("brand", coalesce(col("brand_s"), lit("unknown")))
  }

  def score(kafka: DataFrame): DataFrame =
    withFeatures(withQuality(typed(parse(kafka))))

  val ValidColumns: Seq[String] = Seq(
    "event_time", "event_date", "event_hour", "hour_of_day", "day_of_week",
    "event_type", "product_id", "category_id", "category_code",
    "primary_category", "sub_category", "category_depth",
    "brand", "price", "user_id", "user_session", "dq_flags",
    "kafka_partition", "kafka_offset")

  def validRows(scored: DataFrame): DataFrame =
    scored.filter(col("reject_reason").isNull).select(ValidColumns.map(col): _*)

  def quarantineRows(scored: DataFrame): DataFrame =
    scored.filter(col("reject_reason").isNotNull).select(
      col("kafka_partition"), col("kafka_offset"), col("kafka_timestamp"),
      col("reject_reason"), col("raw_value"), current_timestamp().as("quarantined_at"))
}
