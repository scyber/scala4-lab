import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

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

     //tg.show(10)
     val tg_res = tg.select(col("domain"),col("auto-flag"))
     //tg_res.show(10)
    //val dic_users = broadcast(users.select("id"))
    //val target = domains.withColumn("auto-flag", when(domains.col("user_id") === users.select(col("id")), 1).otherwise(0))
    val rel = tg_res.groupBy(col("domain"),col("auto-flag")).count().where(col("auto-flag") === 1)
    //target.show(10)
    rel.show()

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
