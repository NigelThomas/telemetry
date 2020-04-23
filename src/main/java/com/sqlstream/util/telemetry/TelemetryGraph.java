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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;

public class TelemetryGraph
{
    public static final Logger tracer =
        Logger.getLogger(TelemetryGraph.class.getName());

    static LinkedHashMap<String, Graph> graphHashMap = null;
    static LinkedHashMap<String, Node> nodeHashMap = null;
    static Connection connection = null;
 
    static boolean hideDeletedNodes = true;

    public static void main(String[] args)  {
        tracer.info("Starting");

        int repeatCount = 3; // TODO make this an argument
        long sleepTimeMillis = 10000; // TODO make this an argument

        try {
            connection = DriverManager.getConnection("jdbc:sqlstream:sdp://localhost:5570;user=sa;password=mumble;autoCommit=false");
                    
            // do the time formatting / defaulting in SQL for convenience


            String graphSql = "select CAST(GRAPH_ID AS VARCHAR(8)) AS GRAPH_ID, STATEMENT_ID, SESSION_ID, SOURCE_SQL" +
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
            " from TABLE(SYS_BOOT.MGMT.getStreamGraphInfo(0,0))";

            PreparedStatement graphPs = connection.prepareStatement(graphSql);


            String operatorSql = "select CAST(GRAPH_ID AS VARCHAR(8)) AS GRAPH_ID, NODE_ID" +
                    ", LAST_EXEC_RESULT" +
                    ", NET_INPUT_ROWS, NET_INPUT_BYTES" +
                    ", NET_OUTPUT_ROWS, NET_OUTPUT_BYTES" +
                    ", CAST(COALESCE(INPUT_ROWTIME_CLOCK, CURRENT_TIMESTAMP) AS VARCHAR(32)) AS INPUT_ROWTIME_CLOCK" +
                    ", CAST(COALESCE(OUTPUT_ROWTIME_CLOCK, CURRENT_TIMESTAMP) AS VARCHAR(32)) AS OUTPUT_ROWTIME_CLOCK" + 
                    ", NAME_IN_QUERY_PLAN, QUERY_PLAN, INPUT_NODES" +
            " from TABLE(SYS_BOOT.MGMT.getStreamOperatorInfo(0,0))" +
            " WHERE LAST_EXEC_RESULT <> 'EOS' AND (NAME_IN_QUERY_PLAN NOT LIKE 'StreamSinkPortRel%' AND NAME_IN_QUERY_PLAN NOT LIKE 'NetworkRel%')";
            
            PreparedStatement operatorPs = connection.prepareStatement(operatorSql);


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

                graphHashMap = new LinkedHashMap<>();
                ResultSet graphRs = graphPs.executeQuery();

                while (graphRs.next()) {
                    int col = 1;
                    String graphId = graphRs.getString(col++);
                    int statementId = graphRs.getInt(col++);
                    int sessionId = graphRs.getInt(col++);
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

                    Graph graph = new Graph
                        ( graphId 
                        , statementId
                        , sessionId 
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
                    
                    graphHashMap.put(graphId, graph);
                }
                nodeHashMap = new LinkedHashMap<>();

                ResultSet operRs = operatorPs.executeQuery();
                while (operRs.next()) {
                    int col = 1;
                    String graphId = operRs.getString(col++);
                    String nodeId = operRs.getString(col++);
                    String lastExecResult = operRs.getString(col++);
                    long netInputRows = operRs.getLong(col++);
                    long netInputBytes = operRs.getLong(col++);
                    long netOutputRows = operRs.getLong(col++);
                    long netOutputBytes = operRs.getLong(col++);
                    
                    String inputRowtimeClock = operRs.getString(col++);
                    String outputRowtimeClock = operRs.getString(col++);
                    String nameInQueryPlan = operRs.getString(col++);
                    String queryPlan = operRs.getString(col++);
                    String inputNodes = operRs.getString(col++);

                    Node node = new Node(graphId, nodeId, lastExecResult
                                                    , netInputRows, netInputBytes
                                                    , netOutputRows, netOutputBytes
                                                    , inputRowtimeClock, outputRowtimeClock
                                                    , nameInQueryPlan, queryPlan, inputNodes
                                                    );
                    // retain in a hashmap
                    nodeHashMap.put(nodeId, node);
                }

                // we have the raw data; now link up the nodes with edges and filter out unwanted nodes / edges

                processNodes();

                // and write out all nodes and edges we haven't deleted

                writeNodesAndEdges(i);

             
            }
        } catch (SQLException se) {
            tracer.severe("SQL Exception: "+se.getMessage());
        }  finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException se) {
                    // do nothing
                }
            }
        }

    }

    private static void processNodes() {

        // link up output nodes   
        for (Node node : nodeHashMap.values()) {
            node.linkOutputNodes();
        }

        // now clean out all marked deleted nodes
        // we have to remove deleted child nodes and replace with descendant nodes (if any)

        if (hideDeletedNodes) {
            for (Node node : nodeHashMap.values()) {
                node.unlinkDeleted();
            }
        }

    }

    
       
    private static void writeNodesAndEdges(int i) {
        try (FileWriter fw = new FileWriter(new File("telemetry_"+i+".dot"))) {
            fw.write("digraph {\n");

            // graphs

            for (Graph graph: graphHashMap.values()) {
                fw.write(graph.getDotString());
            }
            // nodes
            for (Node node : nodeHashMap.values()) {
                fw.write(node.getDotString(hideDeletedNodes));
            }

            fw.write("}");
            fw.close();
        } catch (IOException ioe) {
            tracer.log(Level.SEVERE, "Exception writing file #"+i, ioe);
        }
    }

    // constants for dot
    static final String SEMICOLON = ";";
    static final String STARTROW = "<tr><td>";
    static final String NEWCELL = "</td><td>";
    static final String ENDROW = "</td></tr>";
    static final String QUOTE = "\"";
    static final String SPACE = " ";
    static final String NEWLINE = "\n";
    static final String RIGHTARROW = " -> ";
    static final String QUERY = "?";
    static final String INDENT = "    ";

    public static class Graph {
        String graphId;
        int statementId;
        int sessionId;
        String sourceSql;
        String schedState;
        String closeMode;
        boolean isGlobalNexus;
        boolean isAutoClose;
        int numNodes;
        int numLiveNodes;
        int numDataBuffers;
        double totalExecutionTime;
        double totalOpeningTime;
        double totalClosingTime;
        long netInputBytes;
        long netInputRows;
        double netInputRate;
        double netInputRowRate;
        long netOutputBytes;
        long netOutputRows;
        double netOutputRate;
        double netOutputRowRate;
        long netMemoryBytes;
        long maxMemoryBytes;
        String whenOpened;
        String whenStarted;
        String whenFinished;
        String whenClosed;

        protected Graph
            ( String graphId
            , int statementId
            , int sessionId
            , String sourceSql
            , String schedState
            , String closeMode
            , boolean isGlobalNexus
            , boolean isAutoClose
            , int numNodes
            , int numLiveNodes
            , int numDataBuffers
            , double totalExecutionTime
            , double totalOpeningTime
            , double totalClosingTime
            , long netInputBytes
            , long netInputRows
            , double netInputRate
            , double netInputRowRate
            , long netOutputBytes
            , long netOutputRows
            , double netOutputRate
            , double netOutputRowRate
            , long netMemoryBytes
            , long maxMemoryBytes
            , String whenOpened
            , String whenStarted
            , String whenFinished
            , String whenClosed
            ) {
                this.graphId = graphId;
                this.statementId = statementId;
                this.sessionId = sessionId;
                this.sourceSql = sourceSql;
                this.schedState = schedState;
                this.closeMode = closeMode;
                this.isGlobalNexus = isGlobalNexus;
                this.isAutoClose = isAutoClose;
                this.numNodes = numNodes;
                this.numLiveNodes = numLiveNodes;
                this.numDataBuffers = numDataBuffers;
                this.totalExecutionTime = totalExecutionTime;
                this.totalOpeningTime = totalOpeningTime;
                this.totalClosingTime = totalClosingTime;
                this.netInputBytes = netInputBytes;
                this.netInputRows = netInputRows;
                this.netInputRate = netInputRate;
                this.netInputRowRate = netInputRowRate;
                this.netOutputBytes = netOutputBytes;
                this.netOutputRows = netOutputRows;
                this.netOutputRate = netOutputRate;
                this.netOutputRowRate = netOutputRowRate;
                this.netMemoryBytes = netMemoryBytes;
                this.maxMemoryBytes = maxMemoryBytes;
                this.whenOpened = whenOpened;
                this.whenStarted = whenStarted;
                this.whenFinished = whenFinished;
                this.whenClosed = whenClosed;
        }

        protected String getDotString() {
            return INDENT + QUOTE + graphId + QUOTE + SPACE + 
                    "[penwidth=3.0,style=\"bold,filled\",shape=rect,fillcolor=white" +
                   ", label=< <table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"0\"" +
                   ((sourceSql.length() == 0) ? "" : " tooltip=" + QUOTE + StringEscapeUtils.escapeHtml(sourceSql) + QUOTE + " href="+QUOTE+"bogus"+QUOTE) + ">" + 
                   STARTROW+ "Graph ID" + NEWCELL + "State"  + NEWCELL + "Session Id" + NEWCELL + "Statement Id" + ENDROW +
                   STARTROW+ graphId + NEWCELL  + schedState + NEWCELL + sessionId + NEWCELL +  + statementId  + ENDROW +
                   STARTROW+ "Net Mem" + NEWCELL + "Max Mem"  + NEWCELL + "Open Time"  + NEWCELL + "Exec time"  +  ENDROW +
                   STARTROW+ netMemoryBytes+ NEWCELL  + maxMemoryBytes + NEWCELL  + totalOpeningTime + NEWCELL  + totalExecutionTime +  ENDROW +
                   STARTROW+ " " + NEWCELL + "Rows" + NEWCELL + "Row Rate" + NEWCELL + "Bytes" + NEWCELL + "Byte Rate" + ENDROW +
                   STARTROW+ "Input" + NEWCELL +( (netInputRows == 0) ? QUERY : netInputRows) + NEWCELL + netInputRate + NEWCELL + netInputBytes + NEWCELL + netInputRate + ENDROW +
                   STARTROW+ "Output" + NEWCELL + ( (netOutputRows == 0) ? QUERY : netOutputRows) + NEWCELL + netOutputRowRate + NEWCELL + netOutputBytes + NEWCELL + netOutputRate + ENDROW +
                   "</table> >];\n" ;
                
        }

    }


    public static class Node {
        String graphId;
        String nodeId;
        String lastExecResult;
        long netInputRows;
        long netInputBytes;
        long netOutputRows;
        long netOutputBytes;
        String inputRowtimeClock;
        String outputRowtimeClock;
        String nameInQueryPlan;
        String queryPlan;
        String iNodes;
        String[] inputNodes;

        String nodeColor;

        Set<Node> outputNodes = new HashSet<>();

  
        boolean deleted = false;

        // construct with raw data
        protected Node
                        ( String graphId
                        , String nodeId 
                        , String lastExecResult
                        , long netInputRows
                        , long netInputBytes
                        , long netOutputRows
                        , long netOutputBytes
                        , String inputRowtimeClock
                        , String outputRowtimeClock
                        , String nameInQueryPlan
                        , String queryPlan
                        , String inputNodes
                        ){
            this.graphId = graphId;
            this.nodeId = nodeId;
            this.lastExecResult = lastExecResult;
            this.netInputRows = netInputRows;
            this.netInputBytes = netInputBytes;
            this.netOutputRows = netOutputRows;
            this.netOutputBytes = netOutputBytes;
            this.inputRowtimeClock = inputRowtimeClock;
            this.outputRowtimeClock = outputRowtimeClock; 
            this.nameInQueryPlan = nameInQueryPlan;
            this.queryPlan = queryPlan;
            this.iNodes = inputNodes;

            if (inputNodes == null || inputNodes.length() == 0) {
                this.inputNodes = new String[]{};
            } else {
                this.inputNodes = inputNodes.split(SPACE);
            }

            switch (lastExecResult) {
                case "UND":
                    nodeColor = "green";
                    break;
                case "OVR":
                    nodeColor = "red";
                    break;

                case "YLD":
                    nodeColor = "yellow";
                    break;

                case "EOS":
                    // these are excluded in the query
                    // TODO - allow inclusion
                    nodeColor = "blue";
                    break;

                default:
                    nodeColor = "white";
            }

            if (nameInQueryPlan.matches(".*\\.pro") ||
                nameInQueryPlan.matches("\\$Proxy.*")) {

                deleted = true; 
            }

            if (deleted) nodeColor = "gray";

        }

        protected String getNodeId() {
            return nodeId;
        }

        protected boolean isDeleted() {
            return deleted;
        }

        // can only be executed once all nodes are loaded
        protected void addOutputNode(Node outputNode) {
            outputNodes.add(outputNode);
        }
        
        // take all the input nodes in this node and make the corresponding output links from there to here
        protected void linkOutputNodes() {
            
            if (tracer.isLoggable(Level.FINER)) tracer.finer("linkOutputNode: node="+nodeId+", iNodes='"+iNodes+"', list="+Arrays.toString(inputNodes)+", listlen="+inputNodes.length);
            
            for (String inputNode : inputNodes) {
                nodeHashMap.get(inputNode).addOutputNode(this);
            }
        }

        protected String getDotString(boolean hideDeleted) {
           if (deleted && hideDeleted) {
               /* discard deleted elements */
               return "/* "+nodeId+ " */" + NEWLINE;
           } else {
                return getDotNode() + getDotEdges();
           }
        }  


        // TODO - put bytes into human readable form (as for df -h)
        // TODO - put time, rows, bytes into a table

        protected String getDotNode() {
            return INDENT + QUOTE + nodeId + QUOTE + SPACE + 
                    "[penwidth=3.0,style=\"bold,filled\",shape=rect,fillcolor=" +
                   nodeColor + ", label=< <table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"0\"" +
                   ((queryPlan.length() == 0) ? "" : " tooltip=" + QUOTE + StringEscapeUtils.escapeHtml(queryPlan) + QUOTE + " href="+QUOTE+"bogus"+QUOTE) + ">" + 
                   STARTROW+ nodeId + NEWCELL + lastExecResult + "</td><td colspan=\"2\"><B>" + nameInQueryPlan + "</B>" + ENDROW +
                   STARTROW+ "&nbsp;" + NEWCELL + "Time" + NEWCELL + "Rows" + NEWCELL + "Bytes" + ENDROW +
                   STARTROW+ "Input" + NEWCELL + inputRowtimeClock  + NEWCELL +  ( (netInputRows == 0) ? QUERY : netInputRows) + NEWCELL + netInputBytes + ENDROW +
                   STARTROW+ "Output" + NEWCELL + outputRowtimeClock + NEWCELL + ( (netOutputRows == 0) ? QUERY : netOutputRows) + NEWCELL + netOutputBytes +  ENDROW +
                  // NEWCELL + " In Nodes" + NEWCELL +String.join(", ",inputNodes) + ENDROW +
                   "</table> >];\n" ;
                
        }

        protected String getDotEdges() {
            StringBuffer result = new StringBuffer();

            // if first node in graph, link to graph header
            if (iNodes.length() == 0) result.append(INDENT+QUOTE+graphId+QUOTE+RIGHTARROW+QUOTE+nodeId+QUOTE+SEMICOLON+NEWLINE);
            
            // traverse outputNodes as they will have been fixed to remove deleted nodes
            for (Node childNode : outputNodes) {
                result.append(INDENT+INDENT+QUOTE + nodeId + QUOTE + RIGHTARROW + QUOTE + childNode.getNodeId() + QUOTE + SEMICOLON + NEWLINE);
            }

            return result.toString();
        }

        /**
         * getChildren
         *
         * @return a set of (non-deleted) child (output) node ids
         */
         
        protected Set<Node> getChildren(Set<Node> inputSet) {

            for (Node childNode : outputNodes) {
                if (childNode.isDeleted()) {
                    // descend looking for more
                    inputSet.addAll(childNode.getChildren(inputSet));
                } else {
                    // stop here and add node to list of children
                    inputSet.add(childNode);
                }
            }
            if (tracer.isLoggable(Level.FINER)) tracer.finer("node:"+nodeId+", undeleted children:"+nodeSetToString(inputSet));
            return inputSet;
        }

        protected void unlinkDeleted() {
            if (deleted) {
    
                // make a list of the nearest undeleted descendents
    
                Set<Node> childNodes = new HashSet<>();
                childNodes = getChildren(childNodes);
                
                for (String nodeId : inputNodes) {
                    if (tracer.isLoggable(Level.FINEST)) tracer.finest("replacing children for "+nodeId);
                    
                    Node parentNode = nodeHashMap.get(nodeId);
                    parentNode.replaceChildren(this, childNodes);
                }
                
            }
        }
    
        /**
         * Replace children of a parent node; only do it if the parent isn't itself deleted
         * 
         * @param deletedChild - the deleted node to remove / short-circuit
         * @param childNodes - the node(s) to short-circuit to
         */
        protected void replaceChildren(Node deletedChild, Set<Node> childNodes) {
            if (!deleted) {
                if (tracer.isLoggable(Level.INFO)) tracer.info("Node:"+nodeId+" - really replacing deleted "+deletedChild.getNodeId()+" with " + nodeSetToString(childNodes));
                if (tracer.isLoggable(Level.FINEST)) tracer.finest("Set before: "+nodeSetToString(outputNodes));
                
                outputNodes.remove(deletedChild);
                outputNodes.addAll(childNodes);
                
                if (tracer.isLoggable(Level.FINEST)) tracer.finest("Set after: "+nodeSetToString(outputNodes));
            }
        }

        /**
         * Convenience for tracing sets of nodes
         * @param nodeSet
         * @return
         */
        static String nodeSetToString(Set<Node> nodeSet) {
            StringBuffer sb = new StringBuffer("[");
            String delim = "";
            for (Node node : nodeSet) {
                sb.append(delim);
                sb.append(node.getNodeId());
                delim = ", ";
            }
            sb.append("]");
            return sb.toString();
        }
    }


}
