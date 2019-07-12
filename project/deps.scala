import sbt._

object v {
  val spark     = "2.4.3"
  val hadoop    = "2.8.5"
  val jackson   = "2.6.7"
}

object deps {
  // Spark
  val yarn             = "org.apache.spark"                %% "spark-yarn"                    % v.spark force()
  val sql              = "org.apache.spark"                %% "spark-sql"                     % v.spark force()
  val sqlKafka         = "org.apache.spark"                %% "spark-sql-kafka-0-10"          % v.spark force()
  val hive             = "org.apache.spark"                %% "spark-hive"                    % v.spark force()
  val streaming        = "org.apache.spark"                %% "spark-streaming"               % v.spark force()
  val streamingKafka   = "org.apache.spark"                %% "spark-streaming-kafka-0-10"    % v.spark force()
  // Hadoop
  val hadoop           = "org.apache.hadoop"               %  "hadoop-client"                 % v.hadoop force()
  val hadoopAWS        = "org.apache.hadoop"               %  "hadoop-aws"                    % v.hadoop force()
  // logging
  val slf4j            = "org.slf4j"                       %  "slf4j-api"                     % "1.7.25"
  val log4j12          = "org.slf4j"                       %  "slf4j-log4j12"                 % "1.7.16"
  // jackson
  val jacksonAnns      = "com.fasterxml.jackson.core"      % "jackson-annotations"            % v.jackson force()
  val jacksonCore      = "com.fasterxml.jackson.core"      % "jackson-core"                   % v.jackson force()
  val jacksonDataBind  = "com.fasterxml.jackson.core"      % "jackson-databind"               % v.jackson force()
  // misc
  val kryoShaded       = "com.esotericsoftware"            %  "kryo-shaded"                   % "4.0.2"
  val minio            = "io.minio"                        % "minio"                          % "6.0.8"

  // combined
  val sparkEssentials: Seq[ModuleID] = Seq(hadoop, hadoopAWS, kryoShaded)
  val sparkModules: Seq[ModuleID] = Seq(yarn, hive, sql, sqlKafka, streaming, streamingKafka)

  val spark: Seq[ModuleID] = sparkEssentials ++ sparkModules
  val logging: Seq[ModuleID] = Seq(slf4j, log4j12)
  val jackson: Seq[ModuleID] = Seq(jacksonAnns, jacksonCore, jacksonDataBind)
}
