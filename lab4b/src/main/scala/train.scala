import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{
  CountVectorizer,
  IndexToString,
  StringIndexer
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  col,
  collect_list,
  explode,
  from_json,
  udf
}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{
  ArrayType,
  LongType,
  StringType,
  StructField,
  StructType
}

object train {
  val path_to_model = "/tmp/grigory.piskunov"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("grigory_piskunov_lab4b")
      .config("spark.executor.instances", "16")
      .getOrCreate()
    import spark.implicits._
    val parce = udf { (s: Seq[String]) =>
      extractDomains(s)
    }
    val logs = spark.read.json("hdfs:///labs/slaba04b")
    val exp_logs = logs
      .select('gender_age, 'uid, 'visits)
      .withColumn("url", col("visits.url"))
      .withColumn("timestamp", explode(col("visits.timestamp")) as 'timestamp)
      .withColumn("clean_url", parce('url))

    exp_logs.show(5)

    //ToDo aggreage
    val agg_logs = exp_logs.select('uid, 'domains, 'gender_age)
    val train = agg_logs
      .groupBy(agg_logs("gender_age"), col("uid"))
      .agg(collect_list("clean_url"))
      .alias("domains")

    train.show(5)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")
    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(train)

    val labels = new StringIndexer().fit(train).labels

    val converter = new IndexToString()
      .setInputCol("prediction")
      .setLabels(labels)
      .setOutputCol("res")

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, converter))

    val model = pipeline.fit(train)

    model.write.overwrite().save(path_to_model)

    val schema = StructType(
      Seq(
        StructField("uid", StringType, true),
        StructField(
          "visits",
          ArrayType(
            StructType(
              Seq(
                StructField("timestamp", LongType, true),
                StructField("url", StringType, true)
              )
            ),
            true
          ),
          true
        )
      )
    )
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "subscribe" -> "grigory_piskunov"
    )
    val raw_df = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .option("startingOffsets", "earliest")
      .load

    val src_jsdf = raw_df
      .withColumn("jsonData", from_json(col("value").cast("string"), schema))
      .select("jsonData.*")

    val js = src_jsdf
      .select(col("visits"), col("uid"))
      .withColumn("url", col("visits.url"))

    val test_df = js
      .select('uid)
      .groupBy(col("url"))
      .agg(collect_list("url"))
      .alias("domains")

    val test_model = PipelineModel.load(path_to_model)

    val target_df = test_model.transform(test_df)
    //ToDo write to kafka output
    /*
    {"uid": "d50192e5-c44e-4ae8-ae7a-7cfe67c8b777", "gender_age": "F:18-24"}
     */
    //target_df.show(10)
    val sink = target_df.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
    sink.start()

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
  def extractDomains(seg: Seq[String]): Seq[String] =
    seg.map(s => extractDomainName(s))
}
