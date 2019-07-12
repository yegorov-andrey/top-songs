#!/usr/bin/env bash

# call internal docker-spark initialization script
bash /spark/config.sh

# check mandatory environment variables
# those should be passed via Docker CLI
if [ -z ${MASTER+x} ]; then echo "MASTER setting is not set for master mode";exit -1; fi

SPARK_HOME=/spark
LIB=./lib

EXECUTOR_JVM_OPTIONS="-Djava.net.preferIPv4Stack=true"
DRIVER_JVM_OPTIONS="-Djava.net.preferIPv4Stack=true"
HIVE_OUTPUT_FORMAT="orc"

DEFAULT_DRIVER_JVM_OPTIONS="-Djava.net.preferIPv4Stack=true"
DEFAULT_EXECUTOR_JVM_OPTIONS="-Djava.net.preferIPv4Stack=true"

# breaking up long strings into several smaller lines
DRIVER_JAVA_OPTS=" -server "
DRIVER_JAVA_OPTS=" ${DRIVER_JAVA_OPTS} ${DRIVER_JVM_OPTIONS:-$DEFAULT_DRIVER_JVM_OPTIONS}"

EXECUTOR_JAVA_OPTS=" -server "
EXECUTOR_JAVA_OPTS=" ${EXECUTOR_JAVA_OPTS} ${EXECUTOR_JVM_OPTIONS:-$DEFAULT_EXECUTOR_JVM_OPTIONS} "

# classpath entry separator
S=":"
# path to jar file
P="./"

# notice no spaces should be present in classpath string,
# only jar file names and path/separator strings
# one jar per line is most readable and easy to extend
EXECUTOR_CLASSPATH="${P}hadoop-aws-2.8.5.jar"
EXECUTOR_CLASSPATH="${EXECUTOR_CLASSPATH}${S}${P}com.esotericsoftware.kryo-shaded-4.0.2.jar"
EXECUTOR_CLASSPATH="${EXECUTOR_CLASSPATH}${S}${P}commons-logging.commons-logging-1.2.jar"

GLOBAL_SPARK_ARGS="--name \"${APP_NAME}\" \
  --master '${MASTER}' \
  --deploy-mode client \
  --conf spark.ui.port=4040 \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.shuffle.blockTransferService=nio "

CFG_JAVA_OPTS="--driver-java-options \"${DRIVER_JAVA_OPTS}\" \
  --conf spark.executor.extraJavaOptions=\"${EXECUTOR_JAVA_OPTS}\" \
  --jars "$(JARS=("$LIB"/*.jar); IFS=,; echo "${JARS[*]}")" \
  --conf spark.executor.extraClassPath=\"${EXECUTOR_CLASSPATH}\" "

CMD="$SPARK_HOME/bin/spark-submit --verbose $GLOBAL_SPARK_ARGS \
                                            $CFG_JAVA_OPTS \
                                            --class sigma.software.university.ayegorov.TopSongsApp \
                                            $LIB/sigma.software.university.*.jar"

echo "Going to execute command [$CMD]"
bash -c "$CMD"