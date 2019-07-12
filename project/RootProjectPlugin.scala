import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalDeployPlugin
import sbt.Keys._
import sbt._

object RootProjectPlugin extends AutoPlugin {
  override def requires: sbt.Plugins = UniversalDeployPlugin

  override lazy val projectSettings = Seq(
    organization in ThisBuild := "sigma.software.university",

    // do not create top level directory in artifact
    topLevelDirectory := None,

    // do not append Scala versions to the generated artifacts
    crossPaths in ThisBuild := false,

    // use cached version for non changing libraries
    updateOptions in ThisBuild := updateOptions.value.withCachedResolution(cachedResoluton = true),

    fork in ThisBuild := true // If true, forks a new JVM when running
  )
}