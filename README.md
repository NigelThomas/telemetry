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

