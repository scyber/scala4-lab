import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StructField,
  StructType
}

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

object SortDomain {

  def main(args: Array[String]): Unit = {

    val fileResult = "/tmp/lab03b_domains.txt"

    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFCounter")
      .getOrCreate()

    val dataSchema = StructType(
      Array(
        StructField("user_id", IntegerType, true),
        StructField("item_id", IntegerType, true),
        StructField("raiting", IntegerType, true),
        StructField("unix_epoch_time", LongType, true)
      )
    )

    val autousers_location = "lab3b/src/main/resources/users/autousers.json"
    val autousers = ss.read
      .format("json")
      .option("header", "false")
      .load(autousers_location)
      .withColumn("id", explode(col("autousers")))
      .select(col("id"), lit(1).alias("autouser"))

    val raw_autousers =
      ss.read.json("lab3b/src/main/resources/users/autousers.json")

    //val autousers = raw_autousers.select((explode(col("autousers"))).alias("users"))

    //val users = autousers.withColumn("id", autousers.col("users").cast(LongType))

//    val raw_sites = ss.read
//      .option("header", "false")
//      .option("delimiter", "\t")
//      .csv("lab3b/src/main/resources/slaba03b/logs")
//      .toDF("tmp_id", "ts", "url")

    //val sites = raw_sites.withColumn("user_id", raw_sites.col("tmp_id").cast(LongType))

    //val tmp_url = sites
//      .select(col("url"), col("user_id"))
//      .where(col("url").isNotNull and (col("user_id")).isNotNull)

    //val getDomain = udf(extractDomainName _)
    val new_re =
      """^http(s)?(%3A|:)(//|%2F%2F)(www\.)?(([\%a-zA-Zа-яА-Я0-9-]+\.)+[\%a-zA-Zа-яА-Я0-9-]+)\.?/.*$"""

    val logs = ss.read
      .format("csv")
      .option("header", "false")
      .load("lab3b/src/main/resources/slaba03b/logs")
      .withColumn("_tmp", split(col("_c0"), "\\s+"))
      .select(
        col("_tmp").getItem(0).cast("long").as("id"),
        col("_tmp").getItem(1).as("ts"),
        col("_tmp").getItem(2).as("host")
      )
    val logs_domain = logs
      .filter(col("host").isNotNull)
      .filter(substring(col("host"), 0, 4) === "http")
      .withColumn(
        "domain",
        regexp_extract(
          concat(regexp_replace(col("host"), "%2F", "/"), lit("/")),
          new_re,
          5
        )
      )
      .filter(col("domain") !== "")
      .select(col("id"), col("domain"))

    //val domains = tmp_url.select(
    //  col("user_id"),
    //  col("url"),
    //  getDomain(col("url")).alias("domain")
    //)

//    val tg = logs_domain
//      .join(
//        autousers,
//        logs_domain.col("user_id") === autousers.col("id"),
//        "left"
//      )
//      .withColumn(
//        "auto-flag",
//        when(logs_domain.col("user_id") === autousers.col("id"), 1).otherwise(0)
//      )
//      .drop(autousers.columns: _*)

    val result = logs_domain
      .join(autousers, Seq("id"), "left")
      .groupBy(col("domain"))
      .agg(
        count("*").alias("domain_count"),
        sum(col("id")).alias("autouser_count")
      )
      .withColumn(
        "total_auto_logs",
        sum(col("autouser_count")).over(Window.partitionBy(lit(1)))
      )
      .withColumn(
        "metric",
        col("autouser_count") * col("autouser_count") / (col("domain_count") * col(
          "total_auto_logs"
        ))
      )
      .select(
        col("domain"),
        col("metric").cast("Decimal(20,20)").alias("metric")
      )
      .orderBy(desc("metric"), col("domain"))
      .limit(200)

    result.show(10)

    val writer = new BufferedWriter(
      new OutputStreamWriter(new FileOutputStream(fileResult))
    )
    for (line <- result.collect()) {
      writer.write(
        java.net.URLDecoder
          .decode(line(0).asInstanceOf[String], "utf-8") + "\t" + line(1) + "\n"
      )
    }
    writer.close()

  }

  def extractDomainName(s: String): String = {

    val firstPattern = """(?:(?![https]))([a-z.-]+)""".r
    val input = firstPattern.findFirstIn(s)
    val first = input match {
      case Some(v) => {
        v
      }
      case _ => ""
    }
    val secondPattern = """(?:\w(?![www\.]))([a-z.-]+)""".r
    val res = secondPattern.findFirstIn(first) match {
      case Some(v) => v
      case _       => ""
    }
    res
  }
}
