package com.sqlstream.utils.telemetry;

import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

import java.util.logging.Logger;
import java.util.logging.Level;


import org.apache.commons.lang.StringEscapeUtils;


public class Graph {
    public static final Logger tracer =
        Logger.getLogger(Graph.class.getName());

    public static LinkedHashMap<String, Graph> graphHashMap = null;

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


  protected static String lookupSchedState(String schedState) {
        switch (schedState) {
            case "B":
                return "Blocked";

            case "O":
                return "Open";

            case "R":
                return "Runnable";

            case "E":
                return "End of Stream";

            case "C":
                return "Closed";
            
            default:
                return schedState;
        }
    }

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

            graphHashMap.put(graphId, this);
    }

    protected String getDotString() {
        // exclude C and Z states

        if (schedState.equals("C") || schedState.equals("Z")) 
            return "";
        else
            return  INDENT + QUOTE + graphId + QUOTE + SPACE + 
                "[penwidth=3.0,style=\"bold,filled\",shape=rect,fillcolor=white" +
                ", label=< <table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"0\"" +
                ((sourceSql.length() == 0) ? "" : " tooltip=" + QUOTE + StringEscapeUtils.escapeHtml(sourceSql) + QUOTE + " href="+QUOTE+"bogus"+QUOTE) + 
                ">" + 
                STARTROW+ "Graph ID" + NEWCELL + "State"  + NEWCELL + "Session Id" + NEWCELL + "Statement Id" + ENDROW +
                STARTROW+ graphId + NEWCELL  + lookupSchedState(schedState) + NEWCELL + sessionId + NEWCELL +  + statementId  + ENDROW +
                STARTROW+ "Net Mem" + NEWCELL + "Max Mem"  + NEWCELL + "Open Time"  + NEWCELL + "Exec time"  +  ENDROW +
                STARTROW+ Utils.humanReadableByteCountSI(netMemoryBytes,"B")+ NEWCELL  + Utils.humanReadableByteCountSI(maxMemoryBytes,"B") + NEWCELL  + totalOpeningTime + NEWCELL  + totalExecutionTime +  ENDROW +
                STARTROW+ " " + NEWCELL + "Opened" + NEWCELL + "Started"  + NEWCELL + "Closed"  + NEWCELL + "Finished"  +  ENDROW +
                STARTROW+ "Time" + NEWCELL + whenOpened + NEWCELL  + whenStarted + NEWCELL  + whenClosed + NEWCELL  + whenFinished +  ENDROW +
                STARTROW+ " " + NEWCELL + "Rows" + NEWCELL + "Row Rate" + NEWCELL + "Bytes" + NEWCELL + "Byte Rate" + ENDROW +
                STARTROW+ "Input" + NEWCELL +( (netInputRows == 0) ? QUERY : netInputRows) + NEWCELL + netInputRate + NEWCELL + Utils.humanReadableByteCountSI(netInputBytes,"B") + NEWCELL + netInputRate + ENDROW +
                STARTROW+ "Output" + NEWCELL + ( (netOutputRows == 0) ? QUERY : netOutputRows) + NEWCELL + netOutputRowRate + NEWCELL + Utils.humanReadableByteCountSI(netOutputBytes,"B") + NEWCELL + netOutputRate + ENDROW +
                //"<tr><td colspan=\"5\">"+ StringEscapeUtils.escapeHtml(sourceSql) + ENDROW +
                "</table> >];\n" ;
            
    }

}




