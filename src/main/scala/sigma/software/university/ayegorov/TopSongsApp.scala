package sigma.software.university.ayegorov

import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

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

  case class Song(theme: String, title: String, artist: String, year: String, spotifyUrl: String)

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

    val batchId = s"batch_id=${Calendar.getInstance.getTimeInMillis.toString}"
    val path = "s3a://ayegorov/datasets/Top_1_000_Songs_To_Hear_Before_You_Die/*.csv"
    val dataset = prepare(spark, path)
    val transformedUri = transform(dataset, batchId)
    addPartitionToHiveTable(spark, transformedUri, batchId)
    countTopSongsPerYearBefore2000(spark)
  }

  // parsing, validation & cleaning
  def prepare(spark: SparkSession, path: String): Dataset[Song] = {
    val currentYear = Calendar.getInstance.get(Calendar.YEAR)
    spark
      .read
      .option("header", "true")
      // parse
      .csv(path)
      .map((r: Row) => Song(r.getAs(0), r.getAs(1), r.getAs(2), r.getAs(3), r.getAs(4)), Encoders.product[Song])
      // clean
      .map((song: Song) => {
        val year = cleanYear(song.year)
        Song(song.theme, song.title, song.artist, year, song.spotifyUrl)
      }, Encoders.product[Song])
      // validate
      .filter(song =>
        song.title != null
        && song.artist != null
        && Option(song.year).map(_.toInt).exists(a => a >= 1000 && a <= currentYear))
  }

  def cleanYear(year: String): String = {
    val clean = new StringBuffer
    "\\d+".r.findAllMatchIn(year).map(rm => rm.matched).foreach(clean.append)
    clean.toString
  }

  // extract (artist, title, year) and save results to S3
  def transform(dataset: Dataset[Song], batchId: String): String = {
    val transformedUri = s"s3a://$bucket/data/Top_1_000_Songs_To_Hear_Before_You_Die/$batchId"
    dataset
      .map((r: Song) => s"${r.artist}\t${r.title}\t${r.year}", Encoders.STRING)
      .write
      .text(transformedUri)
    log.info(s"Transformed data and saved to $transformedUri")
    transformedUri
  }

  // add new batch in hive table
  def addPartitionToHiveTable(spark: SparkSession, transformedUri: String, batchId: String): Unit = {
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS TopSongs (artist STRING, title STRING, year INTEGER)" +
      " PARTITIONED BY (batch_id string)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'" +
      " STORED AS TEXTFILE" +
      " LOCATION 's3a://ayegorov/data/Top_1_000_Songs_To_Hear_Before_You_Die'")
    sql("ALTER TABLE TopSongs" +
      s" ADD PARTITION (batch_id='$batchId')" +
      s" LOCATION '$transformedUri'")
  }

  // print counts for top songs by year before year 2000
  def countTopSongsPerYearBefore2000(spark: SparkSession): Unit = {
    spark
      .sql("SELECT year, COUNT(*) AS count FROM TopSongs WHERE year < 2000 GROUP BY year ORDER BY year")
      .show(Integer.MAX_VALUE, truncate = false)
  }

  // aggregates top songs per year before year 2000 and saves to separate table
  def aggregateTopSongsPerYearBefore2000(spark: SparkSession): Unit = {
    import spark.sql
    sql("SET hive.exec.reducers.max = 1")
    sql("SET spark.sql.shuffle.partitions = 1")
    sql("CREATE TABLE TopSongsByYearBelow2000" +
      " LOCATION 's3a://ayegorov/hive/Top_1_000_Songs_To_Hear_Before_You_Die/'" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'" +
      " STORED AS TEXTFILE" +
      " AS SELECT year, COUNT(*) AS count FROM TopSongs WHERE year < 2000 GROUP BY year ORDER BY year")
    sql("SELECT * FROM TopSongsByYearBelow2000 ORDER BY count DESC")
      .show(Integer.MAX_VALUE, truncate = false)
  }
}
