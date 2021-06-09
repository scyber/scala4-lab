import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover

object Debuger {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFCounter")
      .getOrCreate()

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = ss
      .createDataFrame(
        Seq(
          (0, Seq("I", "saw", "the", "red", "balloon")),
          (1, Seq("Mary", "had", "a", "little", "lamb"))
        )
      )
      .toDF("id", "raw")
    dataSet.printSchema()

    remover.transform(dataSet).show(false)

  }
}
