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

  def main(args: Array[String]): Unit = {

    val idCourses = List(13702 /*23126 , 21617, 16627, 11556, 11556, 13702*/ )
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
    myDf.show()
    val ruCourses = dfCourses
      .select(col("id"), col("lang"), col("desc"))
      .where(col("lang").equalTo("ru"))
    val allCourses = dfCourses.select(
      col("id"),
      col("lang"),
      col("desc"),
      Process(col("desc")).alias("words")
    )

    val esCourses = dfCourses
      .select(
        col("id"),
        col("lang"),
        col("desc"),
        Process(col("desc")).alias("words")
      )
      .where(col("lang").equalTo("es"))
    val enCourses = dfCourses
      .select(
        col("id"),
        col("lang"),
        col("desc"),
        Process(col("desc")).alias("words")
      )
      .where(col("lang").equalTo("en"))

    val cleanRuCourses = ruCourses.select(
      col("id"),
      col("lang"),
      col("desc"),
      Process(col("desc")).alias("words")
    )
    //cleanRuCourses.show(10)

    val ruRegExpTok = new RegexTokenizer()
      .setInputCol("desc")
      .setOutputCol("word_tok")
      .setPattern("\\W|[а-яА-ЯёЁ]+")

    val remover =
      new StopWordsRemover()
        .setInputCol("word_tok")
        .setOutputCol("cleaned_words")

    val enTokenized = ruRegExpTok.transform(ruCourses)
    //enTokenized.printSchema()
    //enTokenized.show(5)

    val enFiltred = remover.transform(enTokenized)

    enFiltred.show(10)

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(10000)
    val tfRu = hashingTF.transform(enFiltred)
//
    val idfRu = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModelru = idfRu.fit(tfRu)
    val rescaledData = idfModelru.transform(tfRu)
//    rescaledData.printSchema()
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
      .filter(col("rank") between (1, 10))

    resultDF.show(10)

//    val idfRu = new IDF().fit(tfRu)
//    val tfidfRu = idfRu.transform(tfRu)
//    tfRu.show(10)
//    tfidfRu.show(10)

//    println("words with clean")
//    wordsWithClean.show(5)
//    val myCourses = wordsWithClean.select("*").where(col("id").isin(23126, 21617,16627,11556,11556,13702))
//    myCourses.show()
    //println(dfCourses.count())

//    val idf = new IDF().setInputCol("vectorOutput").setOutputCol("features")

//    val featurizedData = hashingTF.transform(cleanWords)
//    println( "------ featurizedData -----")
//    featurizedData.printSchema()
//    println(featurizedData.show(5))
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData)
//    println("----- rescaledData  ------")
//    rescaledData.show(10)
//    rescaledData.printSchema()

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
