import org.apache.spark.ml.feature.{
  HashingTF,
  IDF,
  RegexTokenizer,
  StopWordsRemover,
  Tokenizer
}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.util.regex.Pattern

object TfIDfBuilder {

  case class Result(id: String, array: Array[Int])

  def main(args: Array[String]): Unit = {

    val idCourses = List(23126, 23126 , 21617, 16627, 11556, 16704, 13702 )
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFCounter")
      .getOrCreate()

    val courses =
      ss.read.json("lab2/src/main/resources/DO_record_per_line.json")

    val dfCourses = courses.toDF().select(col("id"), col("lang"), col("desc"))
    val Process = udf(ExtractWords _)
    val myDf = dfCourses.select(col("*")).where(col("id") === 13702)
    val my_lang = myDf.select(col("lang")).collect().head.toString().replaceAll("\\[|\\]", "")

    println(my_lang)



    val allCourses = dfCourses.select(
      col("id"),
      col("lang"),
      col("desc"),
      Process(col("desc")).alias("words")
    ).where(col("lang") === my_lang)


    val regexParsers = my_lang match {
      case "ru" => "\\^[а-яА-ЯёЁ]+"
      case _ => "\\W"
    }

    val ruRegExpTok = new RegexTokenizer()
      .setInputCol("desc")
      .setOutputCol("word_tok")
      .setPattern(regexParsers)

//    val remover =
//      new StopWordsRemover()
//        .setInputCol("word_tok")
//        .setOutputCol("cleaned_words")

    val enTokenized = ruRegExpTok.transform(allCourses)
    //enTokenized.printSchema()
    //enTokenized.show(5)

//    val enFiltred = remover.transform(enTokenized)

    //enFiltred.show(10)

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(10000)
    val tfRu = hashingTF.transform(enTokenized)
//
    val idfRu = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModelru = idfRu.fit(tfRu)
    val rescaledData = idfModelru.transform(tfRu)

    rescaledData.show(10)
    val cosSimilarity = udf { (x: Vector, y: Vector) =>
      val v1 = x.toArray
      val v2 = y.toArray
      val l1 = scala.math.sqrt(v1.map(x => x * x).sum)
      val l2 = scala.math.sqrt(v2.map(x => x * x).sum)
      val scalar = v1.zip(v2).map(p => p._1 * p._2).sum
      scalar / (l1 * l2)
    }
    val count_df = rescaledData
      .filter(col("id").isin(idCourses: _*))
      .select(
        col("id").alias("id_frd"),
        col("features").alias("dense_frd"),
        col("lang").alias("lang_frd")
      )

    val joinedDf = rescaledData
      .join(
        broadcast(count_df),
        col("id") =!= col("id_frd") && col("lang") === col("lang_frd")
      )
      .withColumn(
        "cosine_sim",
        cosSimilarity(col("dense_frd"), col("features"))
      )
    val resultDF = joinedDf
      .select(col("id"), col("lang"), col("cosine_sim"), col("id_frd"))
      .withColumn(
        "cosine_sim",
        when(col("cosine_sim").isNaN, 0).otherwise(col("cosine_sim"))
      )
      .withColumn(
        "rank",
        row_number().over(
          Window.partitionBy(col("id_frd")).orderBy(col("cosine_sim").desc)
        )
      )
      .filter(col("rank") between (2, 11))

    resultDF.show(10)



  }
  def ExtractWords(s: String) = {
    val pattern = Pattern.compile("\\w+|[а-яА-ЯёЁ]+")
    val tmpList = scala.collection.mutable.ListBuffer.empty[String]
    val matcher = pattern.matcher(s.toLowerCase())
    while (matcher.find()) tmpList += matcher.group()

    tmpList.toArray
  }
  def ProcessDF(loc: String) = {}
}
