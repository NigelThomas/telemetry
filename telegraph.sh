#!/bin/bash
#
# execute a query for the telemetry graph

. /etc/sqlstream/environment

HERE=$(dirname $0)

java -cp $SQLSTREAM_HOME/lib/sqlstream-jdbc-complete.jar:$SQLSTREAM_HOME/lib/commons-lang.jar:$SQLSTREAM_HOME/../clienttools/WebAgent/lib/args4j.jar:TelemetryGraph.jar com.sqlstream.utils.telemetry.TelemetryGraph $*
