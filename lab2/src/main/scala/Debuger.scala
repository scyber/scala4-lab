import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import shapeless.HList.ListCompat.::

object Debuger {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFCounter")
      .getOrCreate()

    val src_schema =
      new StructType()
        .add("uid", StringType)
        .add(
          "visits",
          ArrayType(
            new StructType()
              .add("url", StringType)
              .add("timestamp", IntegerType)))

    import ss.implicits._
    val parce = udf { (s: Seq[String]) =>
      extractDomains(s)
    }
    val df = ss.read
      .json("lab2/src/main/resources/sample.json")
      .withColumn("url", col("visits.url"))

    //      .select(col("visits.*"))
    val r = df.withColumn("udftest", parce(col("url")))
    r.show()

    //val r = df.select($"host", callUDF("parce_url", col("url"), lit("HOST")))
    //r.show()
    //.select(('value).cast("string"))
    //.as("js_val")
    //value.count()

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
  def extractDomains(seg: Seq[String]): Seq[String] =
    seg.map(s => extractDomainName(s))

}
