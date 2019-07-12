import deps._

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := "0.1"

lazy val root = (project in file("."))
  .enablePlugins(RootProjectPlugin)
  .enablePlugins(CommandLineProjectPlugin)
  .settings(
    name := "top-songs",
    libraryDependencies ++=
      logging ++ spark ++ jackson,
    mainClass in Compile := Some("sigma.software.university.ayegorov.TopSongsApp"),
    dockerBaseImage := "gemelen/spark:2.4.3-2.12-2.8.5",
    dockerEntrypoint := Seq("bin/start.sh"),
  )
