import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ HashingTF, IDF, Tokenizer }
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.ml.linalg.{ SparseVector, Vector }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SampleSolution {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFCounter")
      .getOrCreate()
    val df = {
      ss.read.json("lab2/src/main/resources/DO_record_per_line.json")
    }
    df.printSchema()
    val cleaned_df = df
      //.withColumn("desc", regexp_replace(col("desc"), "[^\\w\\sа-яА-ЯЁё]", ""))
      .withColumn("desc", lower(trim(regexp_replace(col("desc"), "\\s+", " "))))
      .where(length(col("desc")) > 0)

    val tokenizer = new Tokenizer().setInputCol("desc").setOutputCol("words")
    val wordsDF = tokenizer.transform(cleaned_df)
    def flattenWords = udf((s: Seq[Seq[String]]) => s.flatMap(identity))
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(10000)

    val featurizedData = hashingTF.transform(wordsDF)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")

    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val asDense = udf((v: Vector) => v.toDense)
    val newDf = rescaledData
      .withColumn("dense_features", asDense(col("features")))

    val cosSimilarity = udf { (x: Vector, y: Vector) =>
      val v1 = x.toArray
      val v2 = y.toArray
      val l1 = scala.math.sqrt(v1.map(x => x * x).sum)
      val l2 = scala.math.sqrt(v2.map(x => x * x).sum)
      val scalar = v1.zip(v2).map(p => p._1 * p._2).sum
      scalar / (l1 * l2)
    }

    val id_list = Seq( /*23126, 21617, 16627, 11556, 11556,*/ 13702)
    val filtered_df = newDf
      .filter(col("id").isin(id_list: _*))
      .select(
        col("id").alias("id_frd"),
        col("dense_features").alias("dense_frd"),
        col("lang").alias("lang_frd"))

    val joinedDf = newDf
      .join(
        broadcast(filtered_df),
        col("id") =!= col("id_frd") && col("lang") === col("lang_frd"))
      .withColumn(
        "cosine_sim",
        cosSimilarity(col("dense_frd"), col("dense_features")))

    val filtered = joinedDf
      .filter(col("lang") === "en")
      .withColumn(
        "cosine_sim",
        when(col("cosine_sim").isNaN, 0).otherwise(col("cosine_sim")))
      .withColumn(
        "rank",
        row_number().over(
          Window.partitionBy(col("id_frd")).orderBy(col("cosine_sim").desc)))
      .filter(col("rank") between (2, 11))

    filtered.show(100)

  }
}
