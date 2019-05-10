import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import java.time.LocalDate
import java.time.format.DateTimeFormatter


object RDD {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("SBD_lab1")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    val rawData = sc.textFile("./src/main/resources/data/segment/*.csv")

    val allNames = rawData
      .map(x => x.split("\t"))
      .filter(_.length > 23) // drop rows that do not have allNames column
      .map(col => (LocalDate.parse(col(1).dropRight(6), DateTimeFormatter.ofPattern("yyyyMMdd")),
      col(23).split(",[0-9;]+"))) // format date and split allNames

    val flattenedNames = allNames.flatMap{case(date, names) => names.map(name => ((date, name), 1))}

    val countNames = flattenedNames
      .reduceByKey((x, y) => x + y) // count
      .map{case((date, name), count) => (date, (name, count))}
      .groupByKey() // group by date
      .mapValues(x => x.toList.filter(_._1 != "Type ParentCategory").sortBy(x => -x._2).take(10)) // filter and sort

    countNames.collect().foreach(println)

    spark.stop
  }
}