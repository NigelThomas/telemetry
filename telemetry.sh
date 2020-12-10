. /etc/sqlstream/environment

reps=$1
sleep=$2

: ${reps:=1}
: ${sleep:=5}

for i in `seq 1 $reps`
do
   $SQLSTREAM_HOME/bin/sqllineClient --incremental --run=telemetry.sql > querygraph$i.dot
   if ( "$i" != "$reps" ]
	sleep 5
   fi
done
