import sbt.Keys._
import sbt._

/**
  * Plugin to inject settings relevant for command line applications
  */
object CommandLineProjectPlugin extends AutoPlugin {

  private def date: String = {
    val df = new java.text.SimpleDateFormat("yyyyMMdd'T'HHmmss")
    df setTimeZone java.util.TimeZone.getTimeZone("GMT")
    df format (new java.util.Date)
  }

  override def requires: sbt.Plugins =
    DockerSupportPlugin

  override lazy val projectSettings = Seq(
    exportJars := true, // generate jar file
    version := version.value.toString + "-" + date
  )
}

/**
  * Plugin to inject settings relevant for library artifacts
  */
object LibraryProjectPlugin extends AutoPlugin {
  override lazy val projectSettings = Seq(
    autoScalaLibrary := true,
    exportJars := true // generate jar file
  )
}
