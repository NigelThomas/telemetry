#from collections import namedtuple
#import altair as alt
#import math
#import pandas as pd

import streamlit as st
import html
import json
from pandas import json_normalize

import streamlit as st
import os

#import graphviz

def access(d,k):
    try:
        return str(d[k])
    except KeyError:
        return "N/A"

def html_table_from_lol(lol, tprops):
    sa = [f'<table{tprops}>']
    for sublist in lol:
        sa.append('<tr><td>')
        sa.append('</td><td>'.join(sublist))
        sa.append('</td></tr>')
    sa.append('</table>')

    return ''.join(sa)

def html_thin_table_from_dict(d, tprops, keys):
    sa = [f'<table{tprops}>']
    for (k,v) in d.items():
        if k in keys:
            if type(v) is str:
                # Keep to max 30 chars
                if len(v) > 30:
                    x = v[1:30]+'...'
                else:
                    x = ''.join(v)
                x = html.escape(x)
            else:
                x = str(v)


            sa.append('<tr><td>')
            sa.append(k+'</td><td>'+x)
            sa.append('</td></tr>')
    sa.append('</table>')

    return ''.join(sa)

# distribute k,v where k in `keys` from dict `d` into a list of lists `lol` of width `w`` (which can then be displayed as html)
def node_lol(d, keys, w):
    lol = []
    numrows = (len(keys) + w - 1) // w
    vals = [access(d,k) for k in keys]
    # make a list for interleaving keys and values
    for i in range(numrows):
        first = i*w
        lol.append(keys[first:first+w])
        lol.append(vals[first:first+w])
    return lol

node_keys = [ "nodeId"
            , "lastExecResult"
            , "schedState"
            , "executionCount"
            , "inputRowtimeClock"
            , "netInputRows"
            , "netInputBytes"
            , "netInputRowRate"
            , "netInputRate"
            , "outputRowtimeClock"
            , "netOutputRows"
            , "netOutputBytes"
            , "netOutputRowRate"
            , "netOutputRate"
            , "operation"
            , "nameInQueryPlan"
            , "queryPlan"
            #, "inputNodes"
            #, "numInputNodes"
            #, "outputNodes"
            #, "numOutputNodes"
            , "netMemoryBytes"
            , "maxMemoryBytes"
            , "netScheduleTime"
            , "netExecutionTime"
            ]  

node_table_props = """ border="1" cellborder="1" cellspacing="0" cellpadding="4" """

# generic lookup
def safeLookup(code, names):
    try:
        return names[code]
    except KeyError:
        return code

# translate the schedState into human readable
def lookupSchedState(schedState):
    return safeLookup( schedState, 
                { "B":"Blocked"
                , "C":"Closed"
                , "E":"Finished"
                , "N":"Not runnable"
                , "O":"Open"
                , "R":"Runnable"
                , "R*":"Running"
                , "T":"Suspended"
                , "Z":"Removed"
                })

# translate the status into a human readable string
def lookupStatus(status):
    return safeLookup( status, 
                { "UND":"Need More Input"
                , "OVF":"Back-pressure"
                , "OVR":"Back-pressure"
                , "YLD":"Ready to run"
                , "RUN":"Running"
                , "EOS":"End of Stream"
                })

# cell colors
GOODCOLOR = "green"
ALERTCOLOR = "red"
YELLOWCOLOR = "yellow"
ENDOFSTREAMCOLOR = "gray"


# get a color for each status
def statusColor(status):
    color = safeLookup(status, 
                { "UND":GOODCOLOR
                , "OVF":ALERTCOLOR
                , "OVR":ALERTCOLOR
                , "YLD":YELLOWCOLOR
                , "RUN":GOODCOLOR
                , "EOS":ENDOFSTREAMCOLOR
                })
    if color == status:
        color = YELLOWCOLOR

    return color

# niqp = nameInQueryPlan
# qp = queryplan
# onc = ouputNodeCount

def lookupOperationName(niqp, qp, onc ):

    if niqp == None or len(niqp) == 0:
        return "Unknown"

    planElement = None

    # start with simple mappings
    # TODO what missing XO names are there

    mapDictSimple = { "AspenWindowRel": "Windowed Aggregation"
              , "AspenSortRel": "T-Sort"
              , "AspenStreamingAggregateRel": "Streaming GROUP BY"
              , "AspenStreamTableJoinRel": "Stream/Table Join"
              , "AspenWindowedJoinRel": "Stream/Stream Join"
              , "LhxJoinRel": "Left Join"
              , "FennelValuesRel": "SELECT FROM VALUES(...)"
              , "$Proxy": "Proxy"
              , "StreamSinkPortRel": "StreamSinkPortRel"
              , "NetworkRel": "NetworkRel"
            #   # following have complex processing
            #   , "FarragoJavaUdxRel":"UDX" 
            #   , "AnonymousJavaUdxRel": "Source Foreign Stream"
            #   , "AspenCalcRel": "Projection"
              }

    for k,v in mapDictSimple.items():
        if niqp.startswith(k):
            return v

    if niqp.endswith(".pro"):
        planElement = "Proxy"
    elif niqp.startswith("AspenCalcRel"):
        if "$condition=" in qp:
            planElement = "Projection-Filter";
        else:
            planElement = "Projection";
 
    elif niqp.startswith("FarragoJavaUdxRel"):
        # TODO - distinguish terminal UDX which may be sink foreign stream
        planElement = "UDX";

        if onc == 0:
            planElement = "Sink Foreign Stream";
        
        # extract UDX or sink function name from query plan
        udxPrefix = "FarragoJavaUdxRel(invocation=[CAST(";
        spos = len(udxPrefix)
        epos = qp.find("(", spos)

        if qp.startswith(udxPrefix) and epos > 0:
            planElement = planElement + " " + qp[spos:epos]

    elif niqp.startswith("AnonymousJavaUdxRel"):
        planElement= "Source Foreign Stream";

        udxPrefix = "AnonymousJavaUdxRel(param0=['";
        spos = len(udxPrefix)
        # look for ECDA style
        epos = qp.find("-source", spos);

        # look for SQL MED style
        if epos < 0:
            epos = qp.find("'",spos)

        if qp.startswith(udxPrefix) and epos > 0:
            planElement = planElement + " " + qp[spos : epos]

    else: 
        # just use XO name unchanged
        planElement = niqp

    return planElement

def isRowtimePromoted(qp):
    return "rowtimeSource=[ROWTIME_PROMOTED]" in qp

def transform_node(node):

    # use this dict for scalar items - easy to display
    # nested items will be kept as siblings
    n = {k:v for (k,v) in node.items() if not type(v) is list}

    # define node
    nodeColor = "white"
    isStream = False
    isProxy = False

    nameInQueryPlan = n['nameInQueryPlan']

    if nameInQueryPlan == None or len(nameInQueryPlan) == 0:
        isStream = False
    else:
        isStream = nameInQueryPlan.startswith('[') or nameInQueryPlan.startswith("AnonymousJavaUdxRel")
        if nameInQueryPlan.startswith("FarragoJavaUdxRel",0) and n['numOutputNodes'] == 0:
            isStream = True
        
        if isStream:
            nodeColor = "LightBlue"
        
        if nameInQueryPlan.startswith("$Proxy") or \
           nameInQueryPlan.startswith("StreamSinkPortRel") or \
           nameInQueryPlan.startswith("NetworkRel") or \
           nameInQueryPlan.endswith(".pro"):
            isProxy = True
            nodeColor = "gray";

    n['isStream'] = isStream
    n['isRowtimePromoted'] = isRowtimePromoted(n['queryPlan'])
    n['nodeColor'] = nodeColor
    n['operation'] = lookupOperationName(nameInQueryPlan, n['queryPlan'], n['numOutputNodes'] )

    nx = {"nodeInfo":n, "inputNodeIds":node['inputNodeIds'], "outputNodeIds":node['outputNodeIds']}
    return nx


# transform the format of the server / session / graph / node tree 
def transform_tree(originalTree, deadGraphs, withProxies):
    serverinfo = {k:v for (k,v) in originalTree.items() if not type(v) is list}
    transformedTree = {"server":serverinfo, "sessions":[], "sessionNames":[], 'graphs':[], 'nodes':[] }
    sessions = []

    for session in originalTree['sessions']:
        sessioninfo = {k:v for (k,v) in session.items() if not type(v) is list}
        transformedTree["sessionNames"].append(session["sessionName"])

        s = {'sessionInfo':sessioninfo, 'numGraphs':0, 'numNodes':0, 'graphs':[]}
        graph_list = []
        
        for graph in session['graphs']:
            # only include dead graphs if required
            if deadGraphs or not graph['schedState'].strip() in ('E','Z'):
                s['numGraphs'] += 1
                s['numNodes'] += graph['numNodes']

                ginfo = {k:v for (k,v) in graph.items() if not type(graph[k]) is list}
                ginfo['sessionName'] = session['sessionName']
             
                graph_list.append(ginfo)

                transformedGraph = {"graphInfo":ginfo, "nodes":[]}
                for orignode in graph['nodes']:
                    tn = transform_node(orignode)
                    transformedTree['nodes'].append(tn['nodeInfo'])
                    transformedGraph['nodes'].append(tn)
                
                s['graphs'].append(transformedGraph)
        
        transformedTree['graphs'].extend(graph_list)

        sessions.append(s)
    
    transformedTree['sessions'] = sessions
    return transformedTree


def session_to_dot(s, withProxies, dot):
    #   dot.append('''subgraph cluster_sid%s { style="filled,rounded,dashed,bold"; color=steelblue; fillcolor=lightcyan; fontname="sans-serif"; fontsize=18.0; labeljust=l; labelloc=t; ''' % session['sessionId'])
    edges = {}

    for graph in s['graphs']:
        for node in graph['nodes']:
            nodeinfo = node['nodeInfo']
                        
            # with ltab:
            #     st.write(f"""#### Node {n['nodeId']}""")
            #     #st.text(html_table(node_lol(n, node_keys, 5), node_table_props))
            #     st.code(html_thin_table_from_dict(n, node_table_props, node_keys))

            #the thin table
            dot.append(f""" "{nodeinfo['nodeId']}" [penwidth=2.0,style="bold,filled,rounded", color="#3399FF" fontname="sans-serif", fontsize="12", shape=box,fillcolor="{nodeinfo['nodeColor']}", label=< {html_thin_table_from_dict(nodeinfo, node_table_props, node_keys)} >]; """ )

            # the full caboodle
            # dot.append(f""" "{n['nodeId']}" [penwidth=2.0,style="bold,filled,rounded", color="#3399FF" fontname="sans-serif", fontsize="12", shape=box,fillcolor="{nodeinfo['nodeColor']}", label=< {html_table(node_lol(n, node_keys, 4), node_table_props)} >]; """ )

            # <table border="0" cellborder="0" cellspacing="0" cellpadding="4"><tr><td bgcolor="lightblue" colspan="2"><font point-size="14">
            # <b>{html.escape(nameInQueryPlan)}</b></font></td><td VALIGN="center" bgcolor="green" ><b>{n['schedState']}</b></td></tr><tr><td></td></tr><tr><td></td></tr>
            # <tr><td VALIGN="BOTTOM"><font point-size="14"> <b>Input</b></font><br align="left" />{n['netInputRowRate']} rows/sec, 126 rows total<br align="left" />0 B/sec, 29.45 KB total<br align="left" />Rowtime: 2022-12-08 09:45:24<br align="left" /></td>
            # <td VALIGN="BOTTOM">Execution Count: {n['executionCount']}<br align="center" />CPU Time: {n['netExecutionTime']} ms<br align="center" /><font color="red">Memory Used: {n['netMemoryBytes']} </font><br align="center" /><font color="red">Max Memory Used: {n['maxMemoryBytes']} </font><br align="center" /><font color="red">Selectivity: 0.00%% </font><br align="center" /></td><td VALIGN="BOTTOM"><font point-size="14"> <b>Output</b></font><br align="right" />{n['netOutputRowRate']} rows/sec,  N/A<br align="right" />{n['netOutputRate']} B/sec,  N/A<br align="right" />Rowtime:  N/A<br align="right" /></td></tr></table>            

            # define edges
            # if nodeinfo['numInputNodes'] > 0:
            #     incoming = node['inputNodes'].split(' ')

            thisNodeId = nodeinfo['nodeId']
            for incoming in node['inputNodeIds']:
                k = ''' "%s" -> "%s";''' % (incoming, thisNodeId)
                if not k in edges:
                    # add a key in the dictionary to register it
                    edges[k] = None
                    dot.append(k)

            for outgoing in node['outputNodeIds']:
                k = ''' "%s" -> "%s";''' % (thisNodeId, outgoing)
                if not k in edges:
                    # add a key in the dictionary to register it
                    edges[k] = None
                    dot.append(k)
        
    dot.append("}")

