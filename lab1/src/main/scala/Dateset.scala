import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}

object Dataset {

  //Case class used to create DataSet objects out of the GDelt data.
  //Read the GDelt GKG codebook for more information on the meaning of the fields below.
  case class NewsArticle (
                           identifier: String,
                           timestamp: Timestamp,
                           sourcetype: Integer,
                           source: String,
                           sourceIdentifier: String,
                           counts: String,
                           countsoffset: String,
                           themes: String,
                           enhancedthemes: String,
                           locations: String,
                           locations2: String,
                           persons: String,
                           persons2: String,
                           organizations: String,
                           organizations2: String,
                           tone2: String,
                           dates: String,
                           gcam: String,
                           sharingimage: String,
                           relatedImages: String,
                           socialimageembeds: String,
                           socialvideoembeds: String,
                           quotations: String,
                           allnames: String,
                           amounts: String,
                           translationinfo: String,
                           extras: String

                         )

  def main(args: Array[String]) {

    //Reduce the amount of warnings raised by Spark.
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //Define schema to be able to create Dataframes from the CSV data.
    val schema =
      StructType(
        Array(
          StructField("identifier", StringType, nullable=true),
          StructField("timestamp", TimestampType, nullable=true),
          StructField("sourcetype", IntegerType, nullable=true),
          StructField("source", StringType, nullable=true),
          StructField("sourceIdentifier", StringType, nullable=true),
          StructField("counts", StringType, nullable=true),
          StructField("countsoffset", StringType, nullable=true),
          StructField("themes", StringType, nullable=true),
          StructField("enhancedthemes", StringType, nullable=true),
          StructField("locations", StringType, nullable=true),
          StructField("locations2", StringType, nullable=true),
          StructField("persons", StringType, nullable=true),
          StructField("persons2", StringType, nullable=true),
          StructField("organizations", StringType, nullable=true),
          StructField("organizations2", StringType, nullable=true),
          StructField("tone2", StringType, nullable=true),
          StructField("dates", StringType, nullable=true),
          StructField("gcam", StringType, nullable=true),
          StructField("sharingimage", StringType, nullable=true),
          StructField("relatedImages", StringType, nullable=true),
          StructField("socialimageembeds", StringType, nullable=true),
          StructField("socialvideoembeds", StringType, nullable=true),
          StructField("quotations", StringType, nullable=true),
          StructField("allnames", StringType, nullable=true),
          StructField("amounts", StringType, nullable=true),
          StructField("translationinfo", StringType, nullable=true),
          StructField("extras", StringType, nullable=true)
        )
      )

    //Create a Spark session and Spark context.
    val spark = SparkSession
      .builder
      .appName("SBD_lab1")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    //Import some functions for Datasets.
    import spark.implicits._

    //Read the data out of the loaded CSV files and create NewsArticle Datasets with it.
    val ds = spark.read.format("csv")
      .option("delimiter", "\t")
      .option("mode", "FAILFAST") //If a line cannot be parsed using the schema, then the program shall halt immediately.
      .option("timestampFormat", "yyyyMMddHHmmSS")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load("./src/main/resources/data/segment/*.csv")
      .as[NewsArticle]

    //Only look at articles which a list of mentioned names is provided for.
    val dsFilter = ds.filter(a => a.allnames != null)

    //Only look at the column with the timestamp of the article and the provided names.
    val noUnnecessaryColumnsFilter = dsFilter.map(x => (x.timestamp,x.allnames))

    //Split the concatenated names from each other in the 'allnames' attribute
    val splitNamesMap = noUnnecessaryColumnsFilter.map(x => (x._1, x._2.split(",[0-9;]+")))

    //For every notion of a name, create an entry with the timestamp.
    //The name and the timestamp together can be used to count the amount of notions of the name in the press in a single day.
    val nameFocusFilter = splitNamesMap.flatMap(x => x._2.map(name => ((x._1, name))))
      .filter(x => !(x._2.contains("Category"))) //Remove entries which note '* Category *', as these are 'false positives' and are not really names.

    //Group entries based on the Date-Name combination and reduce these groups to single entries.
    val groupReduction = nameFocusFilter
      .map(x => (x._1.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),x._2))
      .groupByKey(x => x)
      .count()

    //Per date, get the top 10 mentioned names in a list
    val top10hotNames = groupReduction.sort($"count(1)".desc)
      .map{case ((date, name),count) => (date, (name, count))}.rdd //This transformation was necessary at the end because Dataset/Datafram does not provide a function to map values as a list to a key.
      .groupByKey()
      .mapValues(x => x.toList.take(10))

    //Print the most mentioned names.
    top10hotNames.collect().foreach(println)

    //Stop running
    spark.stop
  }
}