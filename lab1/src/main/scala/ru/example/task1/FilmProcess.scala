package ru.example.task1
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}



object FilmProcess {
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
      val data = ss.read.format("csv")
                   .option("header","false").option("delimiter","\t")
                   .schema(dataSchema).load("lab1/src/main/resources/u.data")
                    //.withColumn("date", unix_timestamp(col("date_long")))


     val data_f =  data.withColumn("datetype_timestamp",
        from_unixtime(col("unix_epoch_time"),"yyyy-MM-dd HH:mm:ss"))
      //data_f.show(3)

     val item = ss.read.format("csv").option("header","false").option("delimiter","|").schema(itemSchema).load("lab1/src/main/resources/u.item")
      //item.show(3)


//    val test_film = data_f.select(col("item_id"), col("raiting")).where(col("item_id").equalTo(302) and( col("raiting").equalTo(1)))
//    test_film.show()

    val film_g = data_f.groupBy(col("item_id"), col("raiting")).agg(count("raiting"))
      .where(col("item_id").equalTo(302)).orderBy(col("raiting"))
    film_g.show()
    //toDo to json output


     val films_all = data_f.groupBy( col("raiting")).agg(count("raiting")).orderBy(col("raiting"))
      films_all.show()


    /*
    В поле “hist_film” нужно указать для заданного
    id фильма количество поставленных оценок в следующем порядке: "1", "2", "3", "4", "5".
     */

    //ToDo
    /*
    В поле “hist_all” нужно указать
    то же самое только для всех фильмов общее количество поставленных оценок в том же порядке: "1", "2", "3", "4", "5".
     */

    //ToDo output result json file for my id
    /*
    {
   "hist_film": [
      134,
      123,
      782,
      356,
      148
   ],
   "hist_all": [
      134,
      123,
      782,
      356,
      148
      ]
    }
     */
  }
}
