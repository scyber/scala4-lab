
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.feature._
import org.apache.spark.rdd.RDD

import java.util.Locale
import java.util.regex.Pattern


object TfIDfBuilder {

  def main(args: Array[String]): Unit = {


    val idCourses = List(23126, 21617,16627,11556,11556,13702)
    val ss = SparkSession.builder().master("local[*]").appName("UDFCounter").getOrCreate()
    val courses = ss.read.json("lab2/src/main/resources/DO_record_per_line.json")
    val dfCourses =  courses.toDF().select(col("id"), col("lang"), col("desc"))
    val sampe = dfCourses.select(col("id"),col("lang"),col("desc")).where(col("id").isin(13702))
    sampe.show()

    val ruCourses = dfCourses.select(col("id"),col("lang"),col("desc")).where(col("lang").equalTo("ru"))
    val esCourses = dfCourses.select(col("id"),col("lang"),col("desc")).where(col("lang").equalTo("es"))
    val enCourses = dfCourses.select(col("id"),col("lang"),col("desc")).where(col("lang").equalTo("en"))
    ruCourses.show(5)
    println("--- Spain Courses ---")
    esCourses.show(5)
    println(" --- English courses ----")
    enCourses.show()
    courses.printSchema()
//    dfCourses/*.select(col("id"), col("lang"))*/.show(5)
     val Process = udf(ExtractWords _)
     val cleanRuCourses = ruCourses.select(col("id"),col("lang"),Process(col("desc")).alias("words"))
//    println("--- clean words ----")
     cleanRuCourses.show(5)
     val cleanEnCourses = enCourses.select(col("id"),col("lang"),Process(col("desc")).alias("words"))
     cleanEnCourses.show(5)
     val cleanEsCourses = esCourses.select(col("id"),col("lang"),Process(col("desc")).alias("words"))
     cleanEsCourses.show(5)
//    cleanWords.printSchema()
    val ruLocale = new Locale.Builder().setLanguage("ru").setRegion("ru").build()
    val wordsWithRemoverRu = new StopWordsRemover().setInputCol("words").setLocale("ru_RU").setOutputCol("clean_words").transform(cleanRuCourses)
    wordsWithRemoverRu.show()
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000)
    val tfRu = hashingTF.transform(wordsWithRemoverRu)

    val idfRu = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModelru = idfRu.fit(tfRu)
    val rescaledData = idfModelru.transform(tfRu)
    rescaledData.printSchema()
    rescaledData.show(10)

//    val idfRu = new IDF().fit(tfRu)
//    val tfidfRu = idfRu.transform(tfRu)
    tfRu.show(10)
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
    while (matcher.find())
      tmpList += matcher.group()

    tmpList.toArray
  }
}
