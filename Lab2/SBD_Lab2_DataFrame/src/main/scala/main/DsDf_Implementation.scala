package main

import java.sql.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

object DsDf_Implementation {

  // The class of the GDelt dataset
  case class GDeltData(
                        GKGRECORDID: String,
                        DATE: Date,
                        SourceCollectionIdentifier: Integer,
                        SourceCommonName: String,
                        DocumentIdentifier: String,
                        Counts: String,
                        V2Counts: String,
                        Themes: String,
                        V2Themes: String,
                        Locations: String,
                        V2Locations: String,
                        Persons: String,
                        V2Persons: String,
                        Organizations: String,
                        V2Organizations: String,
                        V2Tone: String,
                        Dates: String,
                        GCAM: String,
                        SharingImage: String,
                        RelatedImages: String,
                        SocialImageEmbeds: String,
                        SocialVideoEmbeds: String,
                        Quotations: String,
                        AllNames: String,
                        Amounts: String,
                        TranslationInfo: String,
                        Extras: String
                      )

  def main(args: Array[String]) {

    // The schema of the GDelt dataset
    val schema: StructType =
      StructType(
        Array(
          StructField("GKGRECORDID", StringType, nullable = true),
          StructField("DATE", DateType, nullable = true),
          StructField("SourceCollectionIdentifier", IntegerType, nullable = true),
          StructField("SourceCommonName", StringType, nullable = true),
          StructField("DocumentIdentifier", StringType, nullable = true),
          StructField("Counts", StringType, nullable = true),
          StructField("V2Counts", StringType, nullable = true),
          StructField("Themes", StringType, nullable = true),
          StructField("V2Themes", StringType, nullable = true),
          StructField("Locations", StringType, nullable = true),
          StructField("V2Locations", StringType, nullable = true),
          StructField("Persons", StringType, nullable = true),
          StructField("V2Persons", StringType, nullable = true),
          StructField("Organizations", StringType, nullable = true),
          StructField("V2Organizations", StringType, nullable = true),
          StructField("V2Tone", StringType, nullable = true),
          StructField("Dates", StringType, nullable = true),
          StructField("GCAM", StringType, nullable = true),
          StructField("SharingImage", StringType, nullable = true),
          StructField("RelatedImages", StringType, nullable = true),
          StructField("SocialImageEmbeds", StringType, nullable = true),
          StructField("SocialVideoEmbeds", StringType, nullable = true),
          StructField("Quotations", StringType, nullable = true),
          StructField("AllNames", StringType, nullable = true),
          StructField("Amounts", StringType, nullable = true),
          StructField("TranslationInfo", StringType, nullable = true),
          StructField("Extras", StringType, nullable = true)
        )
      )

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession
      .builder()
      .appName("SBD_Lab1_Dataset")
      .getOrCreate()

    import spark.implicits._

    // A udf that takes the topic and count sequences and combines them into a list.
    val asList: UserDefinedFunction = udf((topics: Seq[String], counts: Seq[Integer]) => topics.zip(counts).toList)

    //the JSON schema of the topic,count List.
    val finalListSchema: String = "array<struct<topic:string,count:bigint>>"

    // Read the data
    val ds: Dataset[GDeltData] = spark.read
      .format(".csv")
      .option("delimiter", "\t") // Tab is the delimiter
      .option("dateFormat", "yyyyMMddHHmmss") // Dictate the format of the given date.
      .schema(schema)
      .csv(args(0))
      .as[GDeltData]

    // Process the data
    val ds2 = ds
      // Filter out the null column
      .filter(a => a.DATE != null && a.AllNames != null)
      // Take the Date and AllNames columns.
      .select("Date", "AllNames").as[(Date, String)]
      // Flatten them to take the key value pair (Date,topic).
      .flatMap { case (x1, x2) => x2.replaceAll("[,0-9]", "").split(";").map((x1, _))}
      // Filter out the topic Type ParentCategory.
      .filter(!_._2.equals("Type ParentCategory"))
      // Count the times each (date,topic) tuple is in the dataset
      .groupBy('_1, '_2).count
      // Find the rank of each topic in each date
      .withColumn("rank", rank.over(Window.partitionBy('_1).orderBy('count.desc)))
      // Take the top 10 of each Date
      .filter('rank <= 10)
      // Drop the extra column rank
      .drop("rank")
      // Create the expected output in JSON format
      .groupBy('_1 as "data")
      .agg(asList(collect_list('_2), collect_list('count)) as "result")
      .select('data, 'result.cast(finalListSchema))

    ds2.write.format("json").save(args(1))

    spark.stop()

  }

}
