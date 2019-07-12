package sigma.software.university.ayegorov

import java.util.Calendar

import org.apache.spark.sql.Row

object TopSongsProcessor {

  val s3aUrl = "http://minio:9000"
  val s3aAccessKey = "minio"
  val s3aSecretKey = "minio123"
  var bucket = "ayegorov"

  def parseData(r: Row): Song = {
    Song(r.getAs(0), r.getAs(1), r.getAs(2), r.getAs(3), r.getAs(4))
  }

  def cleanData(rec: Song): Song = {
    val year = cleanYear(rec.year)
    Song(rec.theme, rec.title, rec.artist, year, rec.spotifyUrl)
  }

  private def cleanYear(year: String): String = {
    val clean = new StringBuffer
    "\\d+".r.findAllMatchIn(year).map(rm => rm.matched).foreach(clean.append)
    clean.toString
  }

  def validateData(rec: Song): Boolean = {
    val currentYear = Calendar.getInstance.get(Calendar.YEAR)
    rec.title != null &&
      rec.artist != null &&
      Option(rec.year).map(_.toInt).exists(a => a >= 1000 && a <= currentYear)
  }

  def transformData(song: Song): String = {
    s"${song.artist}\t${song.title}\t${song.year}"
  }
}
