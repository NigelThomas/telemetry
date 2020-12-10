# telemetry

This repository contains utilities used to extract and visualize telemetry data from SQLstream s-Server.
* [Basic SQL script](#basic-sql-script)
* [Java program to take multiple snapshots](#java-program-to-take-multiple-snapshots)
* [Detecting blockers](#detecting-blockers)
* [Bottlenecks](#bottlenecks)

## Basic SQL script

* telemetry.sql
* telemetry.sh

Run the `telemetry.sh` script - it connects to the s-Server on localhost and extracts a sequence of dot files called `querygraph_1.dot`, `querygraph_2.dot` etc.

To process the files, use the `dot` tool. You can install dot as part of the `graphviz` package, or you can use a docker image such as `nshine/dot`.

```
./telemetry.sh

for f in querygraph*.dot
do
    echo $f
    cat $f | [ docker container run --rm -i nshine/dot ] dot -Tsvg > $f.svg ; done 
done
```

## Java program to take multiple snapshots

This reads data from the telemetry graph and operator tables and creates a fuller picture of the server state.

```
./telegraph.sh

for f in telemetry_*.dot
do
    echo $f
    cat $f | [ docker container run --rm -i nshine/dot ] dot -Tsvg > $f.svg ; done 
done
```
It is also possible to combine a single pass with the svg generation - particularly if the program is itself being 
run in a container (to avoid generating a file on the container and separately copying it to the host):

```
docker exec -it myappcontainer /home/sqlstream/telegraph.sh -c | docker run -rm -i nshine/dot dot -Tsvg > myapp.svg
```


For help:
```
$ ./telegraph.sh --help

telegraph.sh [OPTIONS...] ARGUMENTS...
 --help                             : print help message and quit (default: true)
 -b (--base-filename) filename      : prefix for path/to/filename - files are also numbered and given a .dot type
                                      (default: telemetry)
 -c (--write-to-console)            : write output to console (stdout) - so can pipe directly to dot (default: false)
 -f (--frequency) frequency         : repeat every <frequency> seconds (default: 10)
 -g (--graph-info-level) info-level : include some graph info at <info-level> (0-4: higher level = more data) for each
                                      stream graph on the first node for that graph (default: 0)
 -p (--show-proxy-nodes)            : include certain proxy nodes that are normally hidden (default: false)
 -r (--repeat-count) repeat-count   : generate <repeat-count> outputs (default: 1)
 -z (--include-dead)                : include dead (zombie) graphs (default: false)
```

**NOTE:** currently -c option gets the output tangled with trace output which is also being logged to the console.


# Detecting Blockers

We can detect blockers by looking for XO nodes that are in UNDerflow state but have input nodes in OVF (overflow)

Use the `telemetry-blockers.sql` script.

```
SELECT ovf.node_id as blocked_node_id, ovf.sched_state as blocked_state
     , und.node_id as blocking_node_id, und.sched_state as blocking_state
     , und.source_sql blocking_sql
FROM (
    SELECT node_id from TABLE(getStreamOperatorInfo(0,1)) 
    WHERE last_exec_result = 'UND'
) und
JOIN (
    SELECT node_id FROM TABLE(getStreamOperatorInfo(0,1))
    WHERE last_exec_result = 'OVF'
    ) ovf
ON ',’||ovf.output_nodes||',' LIKE '%,'||cast(und.node_id AS VARCHAR(5))||',*' 
;
```


# Resource bottlenecks

We can identify the stream graphs and XO nodes consuming the most resources using queries like this:

## Resources by Stream Graph

See https://docs.sqlstream.com/administration-guide/telemetry/#stream-graph-virtual-table-column-types-and-definitions for column definitions.

We start by analyzing at stream graph level.  Each graph corresponds to a running statement; in a production system that generally means a pump, although there may also be client sessions (a remote client inserting or reading data); there will be a session for reading the telemetry api, and one or more internal sessions (one is populating the ALL_TRACE stream, for example).

For cpu time (approximately) use total_execution_time:

SELECT graph_id, total_execution_time, net_input_rows, source_sql
FROM TABLE(getStreamGraphInfo(0,1))
ORDER BY total_execution_time DESC;

For memory, replace total_execution_time with net_memory_bytes (or max_memory_bytes).

The source_sql can be spread over multiple lines of the output so you may wish to leave it out from the list – you can use statement_id instead, and look up the SQL from sys_boot.mgmt.statements_view.

## Resources by stream operator
See https://docs.sqlstream.com/administration-guide/telemetry/#stream-nodes-virtual-table-column-types-and-definitions-getstreamoperatorinfo for column information.

SELECT node_id, net_execution_time, net_input_rows
FROM TABLE(getStreamOperatorInfo(0,1))
ORDER BY net_execution_time DESC

In versions up to and including 7.0.3, the net_input_rows and net_input_bytes columns are not populated on every stream operator; but they are collected for native streams.

You may find that the most resource-consuming stream operator is not a part of the most resource-consuming stream graph.


## Notes
As always, if you are tuning you should concentrate on:

* The biggest consumers of cpu, memory, i/o
* Where you can make the most difference

Change one thing, then re-test. 


