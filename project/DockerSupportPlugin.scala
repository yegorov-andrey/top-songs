import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.Keys.packageName
import com.typesafe.sbt.packager.universal.UniversalDeployPlugin
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport.Universal
import sbt.Keys._
import sbt._

object DockerSupportPlugin extends AutoPlugin {

  override def requires: sbt.Plugins = JavaAppPackaging && UniversalDeployPlugin

  // Settings to push artifacts to artifactory (jars, zip, etc)
  override lazy val projectSettings = Seq(
    dockerUpdateLatest := true,
    mappings in Universal ++= {
      import Path.rebase
      val dir = baseDirectory.value / "scripts"

      (( dir * "*") pair rebase(dir, "bin/"))
    }
  )
}