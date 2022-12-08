#from collections import namedtuple
#import altair as alt
#import math
#import pandas as pd

import streamlit as st
import html
import json
from pandas.io.json import json_normalize

import streamlit as st
import os

#import graphviz

def access(d,k):
    try:
        return str(d[k])
    except KeyError:
        return ""

def html_table(lol, tprops):
    sa = [f'<table{tprops}>']
    for sublist in lol:
        sa.append('<tr><td>')
        sa.append('</td><td>'.join(sublist))
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

node_keys = [ "graphId"
            , "nodeId"
            , "lastExecResult"
            , "schedState"
            , "netInputRows"
            , "netInputBytes"
            , "netInputRowRate"
            , "netInputRate"
            , "netOutputRows"
            , "netOutputBytes"
            , "netOutputRowRate"
            , "netOutputRate"
            , "inputRowtimeClock"
            , "outputRowtimeClock"
            , "nameInQueryPlan"
            , "queryPlan"
            , "inputNodes"
            , "numInputNodes"
            , "outputNodes"
            , "numOutputNodes"
            , "netMemoryBytes"
            , "maxMemoryBytes"
            , "netScheduleTime"
            , "netExecutionTime"
            , "executionCount"
            ]  

node_table_props = """ border="0" cellborder="0" cellspacing="0" cellpadding="4" """

st.sidebar.write('# Telemetry')
st.sidebar.write('## Parameters')

filename = st.sidebar.file_uploader('Choose the file to upload', type='json')
st.write('Selected client file `%s`' % filename)
if filename is not None:
    bytes_data = filename.getvalue()
    tgraph = json.loads(bytes_data.decode('utf-8'))
    st.write('## Telemetry file: ')
    st.write(filename)
    st.json(tgraph, expanded=False)

    st.write("### Server Info")
    server_df = json_normalize(tgraph, max_level=0)
    st.dataframe(server_df)
    st.write("### Graph/Session Info")

    sessions = []

    deadGraphs = st.sidebar.checkbox('Show dead graphs',value=False)

    for session in tgraph['sessions']:
        s = {'sessionName':session['sessionName'], 'numGraphs':0, 'numNodes':0 }
        graph_list = []
        for graph in session['graphs']:
            # only include dead graphs if required
            if deadGraphs or not graph['schedState'].strip() in ('E','Z'):
                s['numGraphs'] += 1
                s['numNodes'] += graph['numNodes']
                g = {k:v for (k,v) in graph.items() if not type(graph[k]) is list}
                g['sessionName'] = session['sessionName']
                graph_list.append(g)
            sessions.append(s)

    session_names = [ s['sessionName'] for s in sessions ]
    sess_name = st.sidebar.selectbox('Choose a session to examine:',session_names)

    st.write("### Node information for session: %s" % sess_name)

    st.dataframe(graph_list)
    node_list = []

    dot = ['''
    digraph {
graph [pad="0.25", ranksep = "1.0", nodesep="1.0"];
edge [color=gray, penwidth=2]; rankdir="LR";
''']

    for session in tgraph['sessions']:
        if session['sessionName'] == sess_name:

            dot.append('''subgraph cluster_sid%s {
 style="filled,rounded,dashed,bold"; color=steelblue; fillcolor=lightcyan; fontname="sans-serif"; fontsize=18.0;
labeljust=l; labelloc=t;
''' % session['sessionId'])

            for graph in session['graphs']:
                for node in graph['nodes']:
                    n = {k:v for (k,v) in node.items() if not type(node[k]) is list}
                    node_list.append(n)

                    # define node
                    nodeColor = "white"
                    isStream = False

                    nameInQueryPlan = n['nameInQueryPlan']

                    if nameInQueryPlan == None or len(nameInQueryPlan) == 0:
                        isStream = False
                    else:
                        isStream = nameInQueryPlan.startswith('[') or nameInQueryPlan.startswith("AnonymousJavaUdxRel")
                        if nameInQueryPlan.startswith("FarragoJavaUdxRel",0) and n['numOutputNodes'] == 0:
                            isStream = True
                        if isStream:
                            nodeColor = "LightBlue"

                    st.text(node_lol(n, node_keys, 4))
                    dot.append(f""" "{n['nodeId']}" 
    [penwidth=2.0,style="bold,filled,rounded", color="#3399FF" fontname="sans-serif", fontsize="12", shape=box,fillcolor="{nodeColor}", 
    label=< {html_table(node_lol(n, node_keys, 4), node_table_props)} >]; """ )

    # <table border="0" cellborder="0" cellspacing="0" cellpadding="4"><tr><td bgcolor="lightblue" colspan="2"><font point-size="14">
    # <b>{html.escape(nameInQueryPlan)}</b></font></td><td VALIGN="center" bgcolor="green" ><b>{n['schedState']}</b></td></tr><tr><td></td></tr><tr><td></td></tr>
    # <tr><td VALIGN="BOTTOM"><font point-size="14"> <b>Input</b></font><br align="left" />{n['netInputRowRate']} rows/sec, 126 rows total<br align="left" />0 B/sec, 29.45 KB total<br align="left" />Rowtime: 2022-12-08 09:45:24<br align="left" /></td>
    # <td VALIGN="BOTTOM">Execution Count: {n['executionCount']}<br align="center" />CPU Time: {n['netExecutionTime']} ms<br align="center" /><font color="red">Memory Used: {n['netMemoryBytes']} </font><br align="center" /><font color="red">Max Memory Used: {n['maxMemoryBytes']} </font><br align="center" /><font color="red">Selectivity: 0.00%% </font><br align="center" /></td><td VALIGN="BOTTOM"><font point-size="14"> <b>Output</b></font><br align="right" />{n['netOutputRowRate']} rows/sec,  N/A<br align="right" />{n['netOutputRate']} B/sec,  N/A<br align="right" />Rowtime:  N/A<br align="right" /></td></tr></table>            
    
                    # define edges
                    if n['numInputNodes'] > 0:
                        incoming = n['inputNodes'].split(' ')
                        for i in incoming:
                            dot.append(''' "%s" -> "%s";''' % (i, n['nodeId']))
            dot.append("    }")
    dot.append("}")
    st.dataframe(node_list)
    st.write('### Telemetry Chart')
    st.text("\n".join(dot))
        
