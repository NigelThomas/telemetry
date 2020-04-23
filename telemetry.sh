. /etc/sqlstream/environment

for i in `seq 1 1`
do
   $SQLSTREAM_HOME/bin/sqllineClient --incremental --run=telemetry.sql > querygraph$i.dot
   sleep 5
done
