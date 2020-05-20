package com.sqlstream.utils.telemetry;

import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.apache.commons.lang.StringEscapeUtils;

public class TelemetryGraph
{
    public static final Logger tracer =
        Logger.getLogger(TelemetryGraph.class.getName());

    Connection connection = null;
 
    @Option(
            name = "--help",
            usage = "print help message and quit",
            required = false)
    private boolean help = false;

    @Option(
        name = "-z",
        aliases = {"--include-dead"},
        usage = "include dead (zombie) graphs",
        required = false)
    private boolean includeDeadGraphs = false;

    // TODO should code for session graph be turned off
    // because we need at least neighboring nodes to be shown

    @Option(
        name = "-s",
        aliases = {"--session-id"},
        usage = "present sub-graph from a single session",
        required = false)
    private int sessionId = 0;

    @Option(
            name = "-f",
            aliases = {"--frequency"},
            usage = "repeat every <frequency> seconds",
            metaVar = "frequency",
            required = false)

    private int sleepPeriod = 10;
    long sleepTimeMillis;

    @Option(
            name = "-r",
            aliases = {"--repeat-count"},
            usage = "generate <repeat-count> outputs",
            metaVar = "repeat-count",
            required = false)

    static private int repeatCount = 1; 

    @Option(
            name = "-g",
            aliases = {"--graph-info-level"},
            usage = "include some graph info at <info-level> (0-4: higher level = more data) for each stream graph on the first node for that graph",
            metaVar = "info-level",
            required = false)
    static private int graphInfoLevel = 0;


    @Option(
            name = "-p",
            aliases = {"--show-proxy-nodes"},
            usage = "include certain proxy nodes that are normally hidden",
            required = false)
    static private boolean showProxyDetail = false;

    @Option(
        name = "-c",
        aliases = {"--write-to-console"},
        usage = "write output to console (stdout) - so can pipe directly to dot",
        required = false)
    static private boolean writeToConsole = false;

    @Option(
        name = "-b",
        aliases = {"--base-filename"},
        usage = "prefix for path/to/filename - files are also numbered and given a .dot type",
        metaVar = "filename",
        required = false)
    static String baseFilename = "telemetry";



    /*
    @Option(
            name = "-su",
            aliases = {"--sqlstream-url"},
            usage = "jdbc URL to the sqlstream server",
            metaVar = "URL",
            required = false)
    private String sqlstreamUrl = "";

    @Option(
            name = "-sn",
            aliases = {"--sqlstream-name"},
            usage = "user name on the sqlstream server",
            metaVar = "NAME",
            required = false)
    private String sqlstreamName = "";

    @Option(
            name = "-sp",
            aliases = {"--sqlstream-password"},
            usage = "user password on the sqlstream server",
            metaVar = "PASSWORD",
            required = false)
    private String sqlstreamPassword = "";
    */

    private void usage(CmdLineParser parser) throws IOException
    {
        System.err.println(
                "telegraph.sh [OPTIONS...] ARGUMENTS...");
        parser.printUsage(System.err);
        System.err.println();
    }

    public void initialize(String[] args) throws IOException
    {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(120);
        try {
            parser.parseArgument(args);
            if (help) {
                usage(parser);
                System.exit(0);
            }
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            usage(parser);
            System.exit(-1);
        }

        sleepTimeMillis = sleepPeriod * 1000;


    }

    public static void main(String[] args)  {
        tracer.info("Starting");

        TelemetryGraph tg = new TelemetryGraph();


        tg.execute(args);
    } 

    void execute(String[] args) {
        try {
            initialize(args);

            connection = DriverManager.getConnection("jdbc:sqlstream:sdp://localhost:5570;user=sa;password=mumble;autoCommit=false");
                    
            // do the time formatting / defaulting in SQL for convenience


            String graphSql = 
                "select CAST(GRAPH_ID AS VARCHAR(8)) AS GRAPH_ID, STATEMENT_ID"+
                    ", g.SESSION_ID, s.SESSION_NAME, SOURCE_SQL" +
                    ",SCHED_STATE,CLOSE_MODE,IS_GLOBAL_NEXUS,IS_AUTO_CLOSE"+
                    ",NUM_NODES,NUM_LIVE_NODES,NUM_DATA_BUFFERS"+
                    ",TOTAL_EXECUTION_TIME,TOTAL_OPENING_TIME,TOTAL_CLOSING_TIME" +
                    ",NET_INPUT_BYTES,NET_INPUT_ROWS,NET_INPUT_RATE,NET_INPUT_ROW_RATE"+
                    ",NET_OUTPUT_BYTES,NET_OUTPUT_ROWS,NET_OUTPUT_RATE,NET_OUTPUT_ROW_RATE"+
                    ",NET_MEMORY_BYTES,MAX_MEMORY_BYTES"+
                    ",CAST(WHEN_OPENED AS VARCHAR(32)) AS WHEN_OPENED"+
                    ",CAST(WHEN_STARTED AS VARCHAR(32)) AS WHEN_STARTED"+
                    ",CAST(WHEN_FINISHED AS VARCHAR(32)) AS WHEN_FINISHED"+
                    ",CAST(WHEN_CLOSED AS VARCHAR(32)) AS WHEN_CLOSED"+
            " from TABLE(SYS_BOOT.MGMT.getStreamGraphInfo("+sessionId+",0)) g" +
            " LEFT JOIN SYS_BOOT.MGMT.SESSIONS_VIEW s ON s.ID = g.SESSION_ID";
            ;


            tracer.info(graphSql.toString());
            PreparedStatement graphPs = connection.prepareStatement(graphSql.toString());

            // try to use same telemetry snapshot, else nodes and graphs may be inconsistent
            // so we assume we can use a 2 second old snapshot (assume 2 < frequency)

            String operatorSql = 
                "select CAST(GRAPH_ID AS VARCHAR(8)) AS GRAPH_ID_STR, NODE_ID" +
                    ", LAST_EXEC_RESULT, SCHED_STATE" +
                    ", NET_INPUT_ROWS, NET_INPUT_BYTES" +
                    ", NET_OUTPUT_ROWS, NET_OUTPUT_BYTES" +
                    ", CAST(COALESCE(INPUT_ROWTIME_CLOCK, CURRENT_TIMESTAMP) AS VARCHAR(32)) AS INPUT_ROWTIME_CLOCK" +
                    ", CAST(COALESCE(OUTPUT_ROWTIME_CLOCK, CURRENT_TIMESTAMP) AS VARCHAR(32)) AS OUTPUT_ROWTIME_CLOCK" + 
                    ", NAME_IN_QUERY_PLAN, QUERY_PLAN" +
                    ", INPUT_NODES, NUM_INPUTS" +
                    ", OUTPUT_NODES, NUM_OUTPUTS" +
            " from TABLE(SYS_BOOT.MGMT.getStreamOperatorInfo("+sessionId+",2)) op" +
            " WHERE (NAME_IN_QUERY_PLAN NOT LIKE 'StreamSinkPortRel%' AND NAME_IN_QUERY_PLAN NOT LIKE 'NetworkRel%')" 
            ;
            
            tracer.info(operatorSql.toString());

            PreparedStatement operatorPs = connection.prepareStatement(operatorSql.toString());

            int graphIdInt = 0;

            // Read N times from statement graph
            for (int i=1; i <= repeatCount; i++) {
                
                if (i > 1) {
                    // wait a bit before re-running
                    tracer.info("Sleeping between iterations");           
                    try {
                        Thread.sleep(sleepTimeMillis);
                    } catch (InterruptedException ie) {
                        tracer.severe("sleep was interruped");
                    }
                }

                tracer.info("Starting iteration "+i);

                Graph.graphHashMap = new LinkedHashMap<>();

                ResultSet graphRs = graphPs.executeQuery();

                while (graphRs.next()) {
                    int col = 1;
                    String graphId = graphRs.getString(col++);
                    int statementId = graphRs.getInt(col++);
                    int sessionId = graphRs.getInt(col++);
                    String sessionName = graphRs.getString(col++);
                    String sourceSql = graphRs.getString(col++);
                    String schedState = graphRs.getString(col++);
                    String closeMode = graphRs.getString(col++);
                    boolean isGlobalNexus = graphRs.getBoolean(col++);
                    boolean isAutoClose = graphRs.getBoolean(col++);
                    int numNodes = graphRs.getInt(col++);
                    int numLiveNodes = graphRs.getInt(col++);
                    int numDataBuffers = graphRs.getInt(col++);
                    double totalExecutionTime = graphRs.getDouble(col++);
                    double totalOpeningTime = graphRs.getDouble(col++);
                    double totalClosingTime = graphRs.getDouble(col++);
                    long netInputBytes = graphRs.getLong(col++);
                    long netInputRows = graphRs.getLong(col++);
                    double netInputRate = graphRs.getDouble(col++);
                    double netInputRowRate = graphRs.getDouble(col++);
                    long netOutputBytes = graphRs.getLong(col++);
                    long netOutputRows = graphRs.getLong(col++);
                    double netOutputRate = graphRs.getDouble(col++);
                    double netOutputRowRate = graphRs.getDouble(col++);
                    long netMemoryBytes = graphRs.getLong(col++);
                    long maxMemoryBytes = graphRs.getLong(col++);
                    String whenOpened = graphRs.getString(col++);
                    String whenStarted = graphRs.getString(col++);
                    String whenFinished = graphRs.getString(col++);
                    String whenClosed = graphRs.getString(col++);

                    //tracer.info("Graph="+graphId+", sessionName="+sessionName);

                    Graph graph = new Graph
                        ( graphId 
                        , statementId
                        , sessionId 
                        , sessionName
                        , sourceSql 
                        , schedState 
                        , closeMode 
                        , isGlobalNexus 
                        , isAutoClose 
                        , numNodes 
                        , numLiveNodes 
                        , numDataBuffers 
                        , totalExecutionTime 
                        , totalOpeningTime 
                        , totalClosingTime 
                        , netInputBytes 
                        , netInputRows 
                        , netInputRate 
                        , netInputRowRate 
                        , netOutputBytes 
                        , netOutputRows 
                        , netOutputRate 
                        , netOutputRowRate 
                        , netMemoryBytes 
                        , maxMemoryBytes 
                        , whenOpened 
                        , whenStarted 
                        , whenFinished 
                        , whenClosed 
                        );
                
                }
                tracer.info("Read a total of "+Graph.graphHashMap.size()+" graphs");

                Node.nodeHashMap = new LinkedHashMap<>();

                
                ResultSet operRs = operatorPs.executeQuery();
                while (operRs.next()) {
                    int col = 1;
                    String graphId = operRs.getString(col++);
                    String nodeId = operRs.getString(col++);
                    String lastExecResult = operRs.getString(col++);
                    String schedState = operRs.getString(col++);
                    long netInputRows = operRs.getLong(col++);
                    long netInputBytes = operRs.getLong(col++);
                    long netOutputRows = operRs.getLong(col++);
                    long netOutputBytes = operRs.getLong(col++);
                    
                    String inputRowtimeClock = operRs.getString(col++);
                    String outputRowtimeClock = operRs.getString(col++);
                    String nameInQueryPlan = operRs.getString(col++);
                    String queryPlan = operRs.getString(col++);
                    String inputNodes = operRs.getString(col++);
                    int numInputNodes = operRs.getInt(col++);
                    String outputNodes = operRs.getString(col++);
                    int numOutputNodes = operRs.getInt(col++);

                    //tracer.info("Node="+nodeId+", graph="+graphId);

                    Node node = new Node(graphId, nodeId, lastExecResult, schedState
                                                    , netInputRows, netInputBytes
                                                    , netOutputRows, netOutputBytes
                                                    , inputRowtimeClock, outputRowtimeClock
                                                    , nameInQueryPlan, queryPlan
                                                    , inputNodes, numInputNodes
                                                    , outputNodes, numOutputNodes
                                                    );
                }

                tracer.info("Read a total of "+Node.nodeHashMap.size()+" nodes (operators)");


                // we have the raw data; now link up the nodes with edges and filter out unwanted nodes / edges
                processNodes();

                // and write out all nodes and edges we haven't deleted
                writeNodesAndEdges(i);
             
            }
        } catch (SQLException se) {
            tracer.log(Level.SEVERE, "SQL Exception", se);
        } catch (IOException ioe) {
            tracer.log(Level.SEVERE, "IO Exception", ioe);

        } finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException se) {
                    // do nothing
                }
            }
        }

    }

    private void processNodes() {

        // link up output nodes   
        for (Node node : Node.nodeHashMap.values()) {
            node.linkOutputNodes(includeDeadGraphs);
        }

        // now clean out all marked deleted nodes
        // we have to remove deleted child nodes and replace with descendant nodes (if any)

        if (!showProxyDetail) {
            for (Node node : Node.nodeHashMap.values()) {
                node.unlinkDeleted();
            }
        }

    }

    
       
    private void writeNodesAndEdges(int i) {
        try (FileWriter fw = (writeToConsole ? new FileWriter( FileDescriptor.out) : new FileWriter(new File(baseFilename+"_"+i+".dot")))) {
            fw.write("digraph {\n");

            /* Exclude graphs - instead we include graph data into first node
            for (Graph graph: Graph.graphHashMap.values()) {
                fw.write(graph.getDotString());
            }
            */

            // nodes
            for (Node node : Node.nodeHashMap.values()) {
                fw.write(node.getDotString(showProxyDetail, graphInfoLevel));
            }

            fw.write("}");
            fw.flush();
            fw.close();
        } catch (IOException ioe) {
            tracer.log(Level.SEVERE, "Exception writing file #"+i, ioe);
        }
    }


}
