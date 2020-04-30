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
    String sessionName;
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

    boolean deleted = true; // don't show graph unless its nodes are being shown

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

  static final String SQLFONT = "<font face=\"courier\" point-size=\"8\">";
  static final String ENDFONT = "</font>";

  
    protected Graph
        ( String graphId
        , int statementId
        , int sessionId
        , String sessionName
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
            this.sessionName = sessionName;
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

    protected void setUndeleted() {
        deleted = false;
    }

    protected static Graph getGraph(String graphId) {
        return graphHashMap.get(graphId);
    }

    /**
     * A row including the SQL - truncated and with the full SQL in a tooltip if necessary
     */
    protected String displaySQL(String sql, int tabCols, int displaySize) {
        String escapedSql = StringEscapeUtils.escapeHtml(sql);
        String startSql = null;
        String fromSql = null;

        if (sql.length() > displaySize) {
            int fromIndex = sql.lastIndexOf("FROM ");
            if (fromIndex < 0) fromIndex = sql.lastIndexOf("from ");
            if (fromIndex > 0) {
                // we want to show the last FROM clause as a second line
                fromSql = sql.substring(fromIndex, sql.length()-1);
                startSql = sql.substring(0, (fromIndex < displaySize) ? fromIndex : displaySize);
            } else {
                startSql = sql.substring(0, displaySize);
            }
        }

        return "<tr><td align=\"left\" colspan=\"" + tabCols + "\"" +
                ((startSql == null) 
                        ? ">" + SQLFONT + escapedSql
                        : (" href=\"bogus\" tooltip=\""+escapedSql+"\">" + SQLFONT + StringEscapeUtils.escapeHtml(startSql)+ " ...")
                ) + 
                ((fromSql == null) ? "" : ("<br/>"+ StringEscapeUtils.escapeHtml((fromSql.length() > displaySize) ? (fromSql.substring(0, displaySize) + " ...") : fromSql ))) +
                ENDFONT + ENDROW;

    }

    protected String getSourceSql() {
        return sourceSql;
    }

    protected String getSchedState() {
        return schedState;
    }

    /**
     * Return a including SQL (unless it matches provided nameInQueryPlan)
     * @param nameInQueryPlan
     * @return
     */
    protected String displaySQL(String nameInQueryPlan) {
        return nameInQueryPlan.equals(sourceSql) ? "" : displaySQL(sourceSql, 4, 120);
    }

    protected String displaySessionName(int tabCols) {
        return (sessionName == null || sessionName.length() == 0) ? "" : 
            (STARTROW + "Session Name: " + "</td><td align=\"left\" colspan=\"" + (tabCols-1) + "\">" +
              SQLFONT + StringEscapeUtils.escapeHtml(sessionName) + ENDFONT + ENDROW);

    }

    protected String getGraphDot(int graphInfoLevel, String nameInQueryPlan) {
        StringBuffer result = new StringBuffer();
        
        result.append(displaySQL(nameInQueryPlan));
        result.append(displaySessionName(4));

        if (graphInfoLevel > 0) {
                result.append(STARTROW+ "Graph ID" + NEWCELL + "State"  + NEWCELL + "Session Id" + NEWCELL + "Statement Id" + ENDROW +
                              STARTROW+ graphId + NEWCELL  + Utils.lookupSchedState(schedState) + NEWCELL + sessionId + NEWCELL +  + statementId  + ENDROW
                );

                if (graphInfoLevel > 1) {
                    result.append(STARTROW+ "Net Mem" + NEWCELL + "Max Mem"  + NEWCELL + "Open Time"  + NEWCELL + "Exec time"  +  ENDROW +
                                  STARTROW+ Utils.humanReadableByteCountSI(netMemoryBytes,"B")+ NEWCELL  + Utils.humanReadableByteCountSI(maxMemoryBytes,"B") + NEWCELL  + totalOpeningTime + NEWCELL  + totalExecutionTime +  ENDROW
                    );

                    if (graphInfoLevel > 2) {
                        result.append(STARTROW+ "When Opened" + NEWCELL + "When Started"  + NEWCELL + "When Closed"  + NEWCELL + "When Finished"  +  ENDROW +
                                    STARTROW+ whenOpened + NEWCELL  + whenStarted + NEWCELL  + whenClosed + NEWCELL  + whenFinished +  ENDROW
                                 );
                        if (graphInfoLevel > 3) {
                            result.append(STARTROW+ "Rows" + NEWCELL + "Row Rate" + NEWCELL + "Bytes" + NEWCELL + "Byte Rate" + ENDROW +
                                        STARTROW+ "Input:"  +( (netInputRows == 0) ? QUERY : Utils.formatLong(netInputRows)) + NEWCELL + Utils.formatDouble(netInputRowRate) + NEWCELL + Utils.humanReadableByteCountSI(netInputBytes,"B") + NEWCELL + Utils.formatDouble(netInputRate) + ENDROW +
                                        STARTROW+ "Output"  + ( (netOutputRows == 0) ? QUERY : Utils.formatLong(netOutputRows)) + NEWCELL + Utils.formatDouble(netOutputRowRate) + NEWCELL + Utils.humanReadableByteCountSI(netOutputBytes,"B") + NEWCELL + Utils.formatDouble(netOutputRate) + ENDROW
                                        );
                    }
                }
            }
        }

        return result.toString();
    }
 

}




