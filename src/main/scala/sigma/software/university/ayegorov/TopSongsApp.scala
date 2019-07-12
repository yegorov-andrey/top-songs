package sigma.software.university.ayegorov

import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import sigma.software.university.ayegorov.TopSongsHiveHelper._
import sigma.software.university.ayegorov.TopSongsProcessor._

object TopSongsApp {

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  private val log = Logger.getLogger(getClass)
  log.setLevel(Level.INFO)

  val s3aUrl = "http://minio:9000"
  val s3aAccessKey = "minio"
  val s3aSecretKey = "minio123"
  var bucket = "ayegorov"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("TopSongs")
      .enableHiveSupport
      .getOrCreate
    val sc = spark.sparkContext

    val hc = sc.hadoopConfiguration
    hc.set("fs.s3a.endpoint", s3aUrl)
    hc.set("fs.s3a.access.key", s3aAccessKey)
    hc.set("fs.s3a.secret.key", s3aSecretKey)
    hc.set("fs.s3a.path.style.access", "true")

    val path = "s3a://ayegorov/datasets/Top_1_000_Songs_To_Hear_Before_You_Die/*.csv"
    val batchId = s"batch_id=${Calendar.getInstance.getTimeInMillis.toString}"
    val outputPath = s"s3a://$bucket/data/Top_1_000_Songs_To_Hear_Before_You_Die/$batchId"

    import spark.sqlContext.implicits._
    spark
      .read
      .option("header", "true")
      .csv(path)
      .rdd
      .map(parseData)
      .map(cleanData)
      .filter(validateData)
      .map(transformData)
      .toDF
      .write
      .text(outputPath)
    log.info(s"Transformed data and saved to $outputPath")

    addBatch(spark.sql, outputPath, batchId)
    countTopSongsPerYearBefore2000(spark.sql)
      .foreach((yearCount: (Int, Long)) => println(s"${yearCount._1} -> ${yearCount._2}"))
  }
}
