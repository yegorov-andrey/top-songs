package sigma.software.university.ayegorov

import org.apache.spark.sql.{DataFrame, Row}


object TopSongsHiveHelper {

  case class SongsPerYear(year: Int, count: Int)

  def addBatch(sql: String => DataFrame, transformedUri: String, batchId: String): Unit = {
    sql("CREATE TABLE IF NOT EXISTS TopSongs (artist STRING, title STRING, year INTEGER)" +
      " PARTITIONED BY (batch_id string)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'" +
      " STORED AS TEXTFILE" +
      " LOCATION 's3a://ayegorov/data/Top_1_000_Songs_To_Hear_Before_You_Die'")
    sql("ALTER TABLE TopSongs" +
      s" ADD PARTITION (batch_id='$batchId')" +
      s" LOCATION '$transformedUri'")
  }

  def countTopSongsPerYearBefore2000(sql: String => DataFrame): List[(Int, Long)] = {
    sql("SELECT year, COUNT(*) AS count FROM TopSongs WHERE year < 2000 GROUP BY year ORDER BY year")
      .collect()
      .map((r: Row) => r.getInt(0) -> r.getLong(1))
      .toList
  }
}
