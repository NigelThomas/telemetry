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

   // constants for dot
   static final String SEMICOLON = ";";
   static final String STARTROW = "<tr><td>";
   static final String NEWCELL = "</td><td>";
   static final String ENDCELL = "</td>";
   static final String ENDROW = "</td></tr>";
   static final String QUOTE = "\"";
   static final String SPACE = " ";
   static final String NEWLINE = "\n";
   static final String RIGHTARROW = " -> ";
   static final String QUERY = "?";
   static final String INDENT = "    ";

   // cell colors
   static final String GREENCELL = "<td bgcolor=\"green\">";
   static final String REDCELL = "<td bgcolor=\"red\">";
   static final String YELLOWCELL = "<td bgcolor=\"yellow\">";
   static final String BLUECELL = "<td bgcolor=\"blue\">";

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

        /*
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
        */ 
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
        outputNodes.add(outputNode);
    }
    
    // take all the input nodes in this node and make the corresponding output links from there to here
    protected void linkOutputNodes() {
        
        if (tracer.isLoggable(Level.FINER)) tracer.finer("linkOutputNode: node="+nodeId+", iNodes='"+iNodes+"', list="+Arrays.toString(inputNodes)+", listlen="+inputNodes.length);
        
        for (String inputNode : inputNodes) {
            try {
                nodeHashMap.get(inputNode).addOutputNode(this);
            } catch (NullPointerException npe) {
                tracer.log(Level.WARNING, "No node '"+ inputNode+"'");
            }
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

    protected String lookupStatus(String in) {
        // Underflow: Input Buffer is empty, Overflow: Output Buffer is full, Yield: Ready to run, RUN: Running
        switch (in) {
            case "UND":
                return GREENCELL+"Input buffer is empty";
            case "OVF":
                return REDCELL+"Overflow - output buffer is full";
            case "YLD":
                return YELLOWCELL+"Ready to run";
            case "RUN":
                return GREENCELL+"Running";
            case "EOS":
                return BLUECELL+"End of Stream";

            default:
                return YELLOWCELL+in;
        }
    }

    // TODO - put bytes into human readable form (as for df -h)
    // TODO - put time, rows, bytes into a table

    protected String getDotNode() {
        return INDENT + QUOTE + nodeId + QUOTE + SPACE + 
                "[penwidth=3.0,style=\"bold,filled\",shape=rect,fillcolor=" +
                nodeColor + ", label=< <table border=\"0\" cellborder=\"1\" cellspacing=\"0\" cellpadding=\"0\"" +
                ((queryPlan.length() == 0) ? "" : " tooltip=" + QUOTE + StringEscapeUtils.escapeHtml(queryPlan) + QUOTE + " href="+QUOTE+"bogus"+QUOTE) + ">" + 
                STARTROW+ nodeId + ENDCELL + lookupStatus(lastExecResult) + ENDCELL + "<td colspan=\"2\"><B>" + nameInQueryPlan + "</B>" + ENDROW +
                STARTROW+ "&nbsp;" + NEWCELL + "Time" + NEWCELL + "Rows" + NEWCELL + "Bytes" + ENDROW +
                STARTROW+ "Input" + NEWCELL + inputRowtimeClock  + NEWCELL +  ( (netInputRows == 0) ? QUERY : Utils.formatLong(netInputRows) ) + NEWCELL + Utils.humanReadableByteCountSI(netInputBytes,"B") + ENDROW +
                STARTROW+ "Output" + NEWCELL + outputRowtimeClock + NEWCELL + ( (netOutputRows == 0) ? QUERY : Utils.formatLong(netOutputRows) ) + NEWCELL + Utils.humanReadableByteCountSI(netOutputBytes,"B") +  ENDROW +
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
            
        } else {
            // mark the corresponding graph as undeleted
            // NOTE: wasteful because every node in the graph so marks it
            Graph.graphHashMap.get(graphId).setUndeleted();
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



