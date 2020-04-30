# telemetry

This repository contains utilities used to extract and visualize telemetry data from SQLstream s-Server.

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
For help:
```
$ ./telegraph.sh --help

telegraph.sh [OPTIONS...] ARGUMENTS...
 --help                             : print help message and quit (default: true)
 -f (--frequency) frequency         : repeat every <frequency> seconds (default: 10)
 -g (--graph-info-level) info-level : include some graph info at <info-level> (0-4: higher level = more data) for each
                                      stream graph on the first node for that graph (default: 0)
 -p (--show-proxy-nodes)            : include certain proxy nodes that are normally hidden (default: false)
 -r (--repeat-count) repeat-count   : generate <repeat-count> outputs (default: 1)
 -z (--include-dead)                : include dead (zombie) graphs (default: false)

```
