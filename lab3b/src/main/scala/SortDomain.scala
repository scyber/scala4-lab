import cats.implicits.catsSyntaxEq
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, LongType, StructField, StructType}

object SortDomain {

  def main(args: Array[String]): Unit = {

    val fileResult = "/tmp/lab03b_domains.txt"

    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFCounter")
      .getOrCreate()

    val dataSchema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("item_id", IntegerType, true),
      StructField("raiting", IntegerType, true),
      StructField("unix_epoch_time", LongType, true))
    )

    val raw_autousers =
      ss.read.json("lab3b/src/main/resources/users/autousers.json")

    val autousers =  raw_autousers.select((explode(col("autousers"))).alias("users"))
    val users = autousers.withColumn("id",autousers.col("users").cast(LongType))



    val raw_sites = ss.read.option("header","false")
                           .option("delimiter","\t")
                           .csv("lab3b/src/main/resources/slaba03b")
      .toDF("tmp_id", "ts", "url")

    val sites = raw_sites.withColumn("user_id", raw_sites.col("tmp_id").cast(LongType))

     val tmp_url = sites.select(col("url"),col("user_id")).where(col("url").isNotNull and(col("user_id")).isNotNull)

     tmp_url.select(col("user_id")).where(col("user_id").isNotNull).show()

     val getDomain = udf(extractDomainName _)


     val domains = tmp_url.select(col("user_id"),col("url"), getDomain(col("url")).alias("domain"))

     val tg = domains.join(users, domains.col("user_id") === users.col("id"), "left")
       .withColumn("auto-flag", when(domains.col("user_id") === users.col("id"),1).otherwise(0))
       .drop(users.columns: _*)
     val dom_counter = tg.count()
     //tg.show(10)
     val tg_auto = tg.groupBy(col("domain").alias("dom-auto"),col("auto-flag")).count().as("auto-vistors").where(col("auto-flag") === 1)

    val all_auto_visitors = tg.select(col("*")).where(tg.col("auto-flag") === 1).count()
    val tg_n_auto = tg.groupBy(col("domain").alias("dom-no-auto"),col("auto-flag")).count().as("no-auto-vistiors").where(col("auto-flag") === 0)
    //tg_n_auto.show(10)
    val tg_f_c = tg.groupBy(col("domain").alias("dom-full")).count().as("all-visitors")
    //tg_f_c.show(10)
    val joined = tg_auto.join(tg_n_auto, tg_auto.col("dom-auto") === tg_n_auto.col("dom-no-auto"))
                        .join(tg_f_c, tg_auto.col("dom-auto") === tg_f_c.col("dom-full"))
      .withColumn("auto-visitors", tg_auto.col("count"))
      .withColumn("no-autovistors", tg_n_auto.col("count"))
      .withColumn("all-vistors", tg_f_c.col("count"))
      .drop(tg_auto.col("count"))
      .drop(tg_n_auto.col("count"))
      .drop(tg_f_c.col("count"))
      .withColumn("proba", pow((col("auto-visitors")/dom_counter),2)/(((col("all-vistors"))*all_auto_visitors))/(dom_counter*dom_counter))

   val sorted =  joined.sort(round(col("proba"),20) desc)
    sorted.show(10)
    println(all_auto_visitors)
    //tmp_url.select(col("url")).where(col("url").like("%ravka%")).show(10)
    //val res = joined.select(col("dom-full"),(pow((col("auto-visitors")/dom_counter),2)/(((col("all-vistors"))*all_auto_visitors))/(dom_counter*dom_counter)).alias("proba")).orderBy(col("proba"),col("dom-full"))
    //res.show(10)
    //val dic_users = broadcast(users.select("id"))
    //val target = domains.withColumn("auto-flag", when(domains.col("user_id") === users.select(col("id")), 1).otherwise(0))

    //target.show(10)



    //val joined = domains.withColumn("aut-flag",1)


     //sample.show()

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
      case _ => ""
    }
    res
  }
}
