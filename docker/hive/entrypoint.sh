#!/bin/bash
set -e

export HADOOP_CLASSPATH="/opt/hive/auxlib/*:/opt/hadoop/share/hadoop/tools/lib/*"
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"

if ! $HIVE_HOME/bin/schematool -dbType postgres -validate > /dev/null 2>&1; then
  echo "Schema não encontrado, inicializando..."
  $HIVE_HOME/bin/schematool -dbType postgres -initSchema
fi

export IS_RESUME=true
exec /entrypoint.sh
