package com.sqlstream.utils.telemetry;

import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

import java.util.logging.Logger;
import java.util.logging.Level;


import org.apache.commons.lang.StringEscapeUtils;
 

public class Node {
    public static final Logger tracer =
        Logger.getLogger(Node.class.getName());

    public static LinkedHashMap<String, Node> nodeHashMap = null;
    String graphId;
    String nodeId;
    String lastExecResult;
    String schedState;
    
    long netInputRows;
    long netInputBytes;
    double netInputRowRate;
    double netInputRate;

    long netOutputRows;
    long netOutputBytes;
    double netOutputRowRate;
    double netOutputRate;

    String inputRowtimeClock;
    String outputRowtimeClock;
    String nameInQueryPlan;
    String queryPlan;
    int numInputNodes;
    double netScheduleTime;
    double netExecutionTime;
    long executionCount;

    String inputNodes;      // flat string representation
    String[] inputNodeIds;  // divided into array
    Set<Node> inputNodeSet = new HashSet<>();

    int numOutputNodes;
    String outputNodes;
    String[] outputNodeIds;
    Set<Node> outputNodeSet = new HashSet<>();

    String nodeColor;

    Graph graph = null;

    boolean deleted = false;

    // constants for dot
    static final String SEMICOLON = ";";
    static final String STARTROW = "<tr><td>";
    static final String STARTCELL = "<td>";
    static final String ENDCELL = "</td>";
    static final String NEWCELL = ENDCELL+STARTCELL;
    static final String ENDROW = "</td></tr>";
    static final String QUOTE = "\"";
    static final String SPACE = " ";
    static final String NEWLINE = "\n";
    static final String RIGHTARROW = " -> ";
    static final String QUERY = "?";
    static final String INDENT = "    ";
    static final String PIPE = "|";

    // cell colors
    static final String GOODCOLOR = "bgcolor=\"green\" ";
    static final String ALERTCOLOR = "bgcolor=\"red\" ";
    static final String YELLOWCOLOR = "bgcolor=\"yellow\" ";
    static final String ENDOFSTREAMCOLOR = " bgcolor=\"gray\" ";

    static final String BOLD = "<b>";
    static final String UNBOLD = "</b>";

   // construct with raw data
    protected Node
                    ( String graphId
                    , String nodeId 
                    , String lastExecResult
                    , String schedState
                    , long netInputRows
                    , long netInputBytes
                    , double netInputRowRate
                    , double netInputRate
                    , long netOutputRows
                    , long netOutputBytes
                    , double netOutputRowRate
                    , double netOutputRate
                    , String inputRowtimeClock
                    , String outputRowtimeClock
                    , String nameInQueryPlan
                    , String queryPlan
                    , String inputNodes
                    , int numInputNodes
                    , String outputNodes
                    , int numOutputNodes
                    , double netScheduleTime
                    , double netExecutionTime
                    , long executionCount
                    ){
        this.graphId = graphId;
        this.nodeId = nodeId;
        this.lastExecResult = lastExecResult;
        this.schedState = schedState.trim();
        this.netInputRows = netInputRows;
        this.netInputBytes = netInputBytes;
        this.netInputRowRate = netInputRowRate;
        this.netInputRate = netInputRate;
        this.netOutputRows = netOutputRows;
        this.netOutputBytes = netOutputBytes;
        this.netOutputRowRate = netOutputRowRate;
        this.netOutputRate = netOutputRate;
        this.inputRowtimeClock = inputRowtimeClock;
        this.outputRowtimeClock = outputRowtimeClock; 
        this.nameInQueryPlan = nameInQueryPlan;
        this.queryPlan = queryPlan;
        this.numInputNodes = numInputNodes;
        this.inputNodes = inputNodes;
        this.numOutputNodes = numOutputNodes;
        this.outputNodes = outputNodes;
        this.netExecutionTime = netExecutionTime;
        this.netScheduleTime = netScheduleTime;
        this.executionCount = executionCount;

        if (numInputNodes == 0 || inputNodes == null || inputNodes.length() == 0) {
            this.inputNodeIds = new String[]{};
        } else {
            this.inputNodeIds = inputNodes.split(SPACE);
        }

        if (numOutputNodes == 0 || outputNodeIds == null || outputNodes.length() == 0) {
            this.outputNodeIds = new String[]{};
        } else {
            this.outputNodeIds = outputNodes.split(SPACE);
        }


        nodeColor = "white";

        if (nameInQueryPlan.matches(".*\\.pro") ||
            nameInQueryPlan.matches("\\$Proxy.*")) {

            deleted = true; 
        }

        if (deleted) nodeColor = "gray";

        // add to the map
        nodeHashMap.put(nodeId, this);
    }

    protected String getNodeId() {
        return nodeId;
    }

    protected boolean isDeleted() {
        return deleted;
    }

    // can only be executed once all nodes are loaded
    protected void addOutputNode(Node outputNode) {
        outputNodeSet.add(outputNode);
    }
    
    // take all the input nodes in this node and make the corresponding output links from there to here
    protected void linkOutputNodes(boolean includeDeadGraphs) {
        
        if (tracer.isLoggable(Level.FINER)) tracer.finer("linkOutputNode: node="+nodeId+", inputNodes='"+inputNodes+"', list="+Arrays.toString(inputNodeIds)+", listlen="+inputNodeIds.length);
        
        this.graph = Graph.getGraph(graphId);
        if (graph == null) {
            tracer.warning("Node "+nodeId+" cannot find corresponding graph "+graphId);
        } else if (!includeDeadGraphs && graph.getSchedState().equals("Z")) {
            // don't show any nodes in a dead graph
            tracer.finer("node "+nodeId+" excluded - dead graph");
            this.deleted = true;
        } else {
            // remove unwanted proxy nodes from graph 

            for (String inputNodeId : inputNodeIds) {
                try {
                    nodeHashMap.get(inputNodeId).addOutputNode(this);
                } catch (NullPointerException npe) {
                    tracer.log(Level.WARNING, "No input node '"+ inputNodeId+"' ref from '"+nodeId+"'");
                    tracer.log(Level.INFO,this.toString());
                }
            }

            //  structural checks against output nodes
            for (String outputNodeId : outputNodeIds) {
                try {
                    Node onode = nodeHashMap.get(outputNodeId);
                    if (onode == null) throw new NullPointerException("output node "+outputNodeId);
                } catch (NullPointerException npe) {
                    tracer.log(Level.WARNING, "No output node '"+ outputNodeId+"' ref from '"+nodeId+"'");
                    tracer.log(Level.INFO,this.toString());
                }
            }
        }
    }

    protected String getDotString(boolean showDeleted, int graphInfoLevel) {
        if (deleted && !showDeleted) {
            /* discard deleted elements */
            return "/* "+nodeId+ " */" + NEWLINE;
        } else {
            return getDotNode(graphInfoLevel) + getDotEdges();
        }
    }  

    /**
     * Provide a human readable interpretation of Last_Execution_Status
     * @param in
     * @return
     */
    protected String lookupStatus(String in) {
        // Underflow: Input Buffer is empty, Overflow: Output Buffer is full, Yield: Ready to run, RUN: Running
        switch (in) {
            case "UND":
                return "Input buffer is empty";
            case "OVF":
                return "Overflow - output buffer is full";
            case "YLD":
                return "Ready to run";
            case "RUN":
                return "Running";
            case "EOS":
                return "End of Stream";

            default:
                return in;
        }
    }

        /**
     * Return the background color based on status
     * @param status last_execution_status
     * @return
     */
    protected String statusColor(String status) {
        switch (status) {
            case "UND":
                return GOODCOLOR;
            case "OVF":
                return ALERTCOLOR;
            case "RUN":
                return GOODCOLOR;
            case "EOS":
                return ENDOFSTREAMCOLOR;
            case "YLD":
            default:
                return YELLOWCOLOR;
        }
    }

    /**
     * Returns the operation description (and the operation as a tooltip)
     * includes opening the <td> but not closing it
     * @param in
     * @return
     */
    protected String lookupOperation(String in, int colSpan) {
        String planElement = null;

        if (in.startsWith("AspenCalcRel",0)) {
            planElement = "Projection/Filter";
        } else if (in.startsWith("AspenWindowRel",0)) {
            planElement = "Windowed Aggregation";
        } else if (in.startsWith("AspenSortRel",0)) {
            planElement = "T-Sort";
        } else if (in.startsWith("AspenStreamingAggregateRel",0)) {
            planElement = "Streaming GROUP BY";
        } else if (in.startsWith("AspenStreamTableJoinRel",0)) {
            planElement = "Stream/Table Join";
        } else if (in.startsWith("AspenWindowedJoinRel",0)) {
            planElement = "Stream/Stream Join";
        } else if (in.startsWith("LhxJoinRel",0)) {
            planElement = "Left Join";
        } else if (in.startsWith("FarragoJavaUdxRel",0)) {
            // TODO - distinguish terminal UDX which may be sink foreign stream
            planElement = "UDX";
            Set<Node> children = new HashSet<Node>();
            children = getChildren(children);
            if (children.isEmpty()) {
                planElement = "Sink Foreign Stream";
            }
        } else if (in.startsWith("AnonymousJavaUdxRel",0)) {
            planElement= "Source Foreign Stream";
        } 
        
        if (planElement == null) {
            // TODO get a complete list of operation names
            return "<td colspan=\""+colSpan+ "\">" + BOLD + in + UNBOLD;
        } else {
            return "<td colspan=\""+colSpan+"\" href=\"bogus\" tooltip=\"" +in +"\">" + BOLD + planElement + UNBOLD;
        }

    }

    /**
     * Return nodeId with the list of input nodes as a tooltip
     * @return
     */
    String nodeCell() {
        return "<td tooltip=\""+ inputNodes+"\" href=\"bogus\">" + nodeId + ENDCELL;
    }

    // TODO - put bytes into human readable form (as for df -h)
    // TODO - put time, rows, bytes into a table

    protected String getDotNode(int graphInfoLevel) {
        Graph graph = null;
        
        // get graphId if this is the first node in a graph
        // it is the first node if there are no input nodes, or if all input nodes are from other graphs
        // TODO do we need to maintain the inputNodes (and/or inputNodes) as we delete nodes from the graph?

        if ((inputNodes.length() == 0 || !(" "+inputNodes).matches(".*"+graphId+"\\..*"))) {
            graph = Graph.getGraph(graphId);

        }

        nodeColor = "white";
        boolean isStream = nameInQueryPlan.startsWith("[") || nameInQueryPlan.startsWith("AnonymousJavaUdxRel");
        if (nameInQueryPlan.startsWith("FarragoJavaUdxRel",0)) {
            Set<Node> children = new HashSet<Node>();
            children = getChildren(children);
            if (children.isEmpty()) {
                isStream = true;
            }
        }

        if (isStream) {
            nodeColor = "LightBlue";
        }

        // graphInfoLevel > 1 => 6 columns
        // graphInfoLevel <= 1 => 4 columns
        boolean show6cols = (graphInfoLevel > 1);
        int lookupColspan = (show6cols?4:2);

        return INDENT + QUOTE + nodeId + QUOTE + SPACE + 
                "[penwidth=3.0,style=\"bold,filled\",shape=rect,fillcolor=" +
                nodeColor + ", label=< <table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"8\"" +
                ((queryPlan.length() == 0) ? "" : " tooltip=" + QUOTE + StringEscapeUtils.escapeHtml(queryPlan) + QUOTE + " href="+QUOTE+"bogus"+QUOTE) + ">" + 
                "<tr>" + nodeCell() + lookupOperation(nameInQueryPlan,lookupColspan) + ENDCELL + "<td " + statusColor(lastExecResult) + ">" + Utils.lookupSchedState(schedState) + "<br/>" + lookupStatus(lastExecResult) + ENDROW +
                STARTROW+ ((show6cols) ? "Sched Time:" + NEWCELL + Utils.formatDouble(netScheduleTime) + NEWCELL : "")  
                     + "Exec Time: " + NEWCELL + Utils.formatDouble(netExecutionTime) + NEWCELL + "Exec Count" + NEWCELL + Utils.formatLong(executionCount) +  ENDROW +
                STARTROW+ " " + NEWCELL + "Rowtime" + 
                       ((show6cols) ? NEWCELL + "Bytes" + NEWCELL + "Bytes/sec" : "") +
                       NEWCELL + "Rows" + NEWCELL+ "Rows/sec" + ENDROW +
                STARTROW+ "Input: " + NEWCELL + inputRowtimeClock + NEWCELL + 
                     ((show6cols) ? Utils.humanReadableByteCountSI(netInputBytes,"B") + NEWCELL + Utils.formatDouble(netInputRate) + NEWCELL : "") +  
                     ( (netInputRows == 0) ? QUERY : Utils.formatLong(netInputRows) )  + NEWCELL + Utils.formatDouble(netInputRowRate) +  ENDROW +
                STARTROW+ "Output: " + NEWCELL + outputRowtimeClock + NEWCELL + 
                     ((show6cols) ? Utils.humanReadableByteCountSI(netOutputBytes,"B")  + NEWCELL + Utils.formatDouble(netOutputRate)+ NEWCELL : "")  +
                     ( (netOutputRows == 0) ? QUERY : Utils.formatLong(netOutputRows) ) + NEWCELL + Utils.formatDouble(netOutputRowRate) +  ENDROW +
                // if first node in graph, include remaining graph details            
                ((graph != null) ? graph.getGraphDot(graphInfoLevel, nameInQueryPlan) : "") +
                "</table> >];\n" ;
            
    }

    protected String getDotEdges() {
        StringBuffer result = new StringBuffer();

        
        // traverse outputNodes as they will have been fixed to remove deleted nodes
        for (Node childNode : outputNodeSet) {
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

        for (Node childNode : outputNodeSet) {
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
            
            for (String nodeId : inputNodeIds) {
                if (tracer.isLoggable(Level.FINEST)) tracer.finest("replacing children for "+nodeId);
                
                Node parentNode = nodeHashMap.get(nodeId);
                if (parentNode == null) {
                    tracer.warning("no node for "+nodeId);
                } else {
                    parentNode.replaceChildren(this, childNodes);
                }
            }
            
        } else {
            // mark the corresponding graph as undeleted
            // NOTE: wasteful because every node in the graph so marks it
            if (Graph.graphHashMap.get(graphId) != null) {
                Graph.graphHashMap.get(graphId).setUndeleted();
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
            if (tracer.isLoggable(Level.FINE)) tracer.fine("Node:"+nodeId+" - really replacing deleted "+deletedChild.getNodeId()+" with " + nodeSetToString(childNodes));
            if (tracer.isLoggable(Level.FINEST)) tracer.finest("Set before: "+nodeSetToString(outputNodeSet));
            
            outputNodeSet.remove(deletedChild);
            outputNodeSet.addAll(childNodes);
            
            if (tracer.isLoggable(Level.FINEST)) tracer.finest("Set after: "+nodeSetToString(outputNodeSet));
        }
    }

    @Override
    public String toString() {
        return "Node:"+nodeId+PIPE+"Graph:"+graphId+PIPE+
            "schedState:"+schedState+PIPE+"lastExecResult:"+lastExecResult+PIPE+nameInQueryPlan;
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



