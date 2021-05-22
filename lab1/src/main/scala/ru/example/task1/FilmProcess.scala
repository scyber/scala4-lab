package ru.example.task1
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import io.circe.generic.auto._
import io.circe.syntax._

import java.io.{File, PrintWriter}


object FilmProcess {
  case class Raiting( hist_film: Array[Int], hist_all: Array[Int]) {

  }

  def main(args: Array[String]): Unit = {
      val ss = SparkSession.builder().master("local[*]").appName("FilmProcessor").getOrCreate()
      val dataSchema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("item_id", IntegerType, true),
      StructField("raiting", IntegerType, true),
      StructField("unix_epoch_time", LongType, true))
    )

    val itemSchema = StructType(Array(
      StructField("item_id",IntegerType,true),
      StructField("item_title",StringType,true),
      StructField("release_date",StringType,true),
    ))
      val idFilm = 302
      val data_src = ss.read.format("csv")
                   .option("header","false").option("delimiter","\t")
                   .schema(dataSchema).load("lab1/src/main/resources/u.data")



     val data =  data_src.withColumn("datetype_timestamp",
        from_unixtime(col("unix_epoch_time"),"yyyy-MM-dd HH:mm:ss"))
      //data_f.show(3)

     val item = ss.read.format("csv").option("header","false").option("delimiter","|").schema(itemSchema).load("lab1/src/main/resources/u.item")
      //item.show(3)


    val film_g = data.groupBy(col("item_id"), col("raiting")).agg(count("raiting").alias("agg_raiting"))
      .where(col("item_id").equalTo(idFilm))

    val res_film = film_g.join(item, film_g.col("item_id").equalTo(item.col("item_id"))).orderBy(col("raiting"))
                          .drop(item.col("item_id"))

    val film_data = res_film.select(col("agg_raiting")).collect().map(x => x.toSeq).flatten.map(x => x.toString.toInt)

     val films_all = data.groupBy( col("raiting")).agg(count("raiting").alias("agg_rating")).orderBy(col("raiting"))

    val hist_all = films_all.select(col("agg_rating")).collect().map(x =>x.toSeq).flatten.map(x => x.toString.toInt)

    val res = Raiting(film_data,hist_all)

    val outputPath = "/tmp/lab01.json"

    val writer = new PrintWriter(new File(outputPath))
    writer.write(res.asJson.toString())
    writer.close()





  }
}
