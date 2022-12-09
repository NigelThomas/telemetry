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

def session_to_dot(s, withProxies, dot, node_list):
    #   dot.append('''subgraph cluster_sid%s { style="filled,rounded,dashed,bold"; color=steelblue; fillcolor=lightcyan; fontname="sans-serif"; fontsize=18.0; labeljust=l; labelloc=t; ''' % session['sessionId'])
    edges = {}

    for graph in s['graphs']:
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
                        
                # with ltab:
                #     st.write(f"""#### Node {n['nodeId']}""")
                #     #st.text(html_table(node_lol(n, node_keys, 5), node_table_props))
                #     st.code(html_thin_table_from_dict(n, node_table_props, node_keys))

                #the thin table
                dot.append(f""" "{n['nodeId']}" [penwidth=2.0,style="bold,filled,rounded", color="#3399FF" fontname="sans-serif", fontsize="12", shape=box,fillcolor="{nodeColor}", label=< {html_thin_table_from_dict(n, node_table_props, node_keys)} >]; """ )

                # the full caboodle
                # dot.append(f""" "{n['nodeId']}" [penwidth=2.0,style="bold,filled,rounded", color="#3399FF" fontname="sans-serif", fontsize="12", shape=box,fillcolor="{nodeColor}", label=< {html_table(node_lol(n, node_keys, 4), node_table_props)} >]; """ )

    # <table border="0" cellborder="0" cellspacing="0" cellpadding="4"><tr><td bgcolor="lightblue" colspan="2"><font point-size="14">
    # <b>{html.escape(nameInQueryPlan)}</b></font></td><td VALIGN="center" bgcolor="green" ><b>{n['schedState']}</b></td></tr><tr><td></td></tr><tr><td></td></tr>
    # <tr><td VALIGN="BOTTOM"><font point-size="14"> <b>Input</b></font><br align="left" />{n['netInputRowRate']} rows/sec, 126 rows total<br align="left" />0 B/sec, 29.45 KB total<br align="left" />Rowtime: 2022-12-08 09:45:24<br align="left" /></td>
    # <td VALIGN="BOTTOM">Execution Count: {n['executionCount']}<br align="center" />CPU Time: {n['netExecutionTime']} ms<br align="center" /><font color="red">Memory Used: {n['netMemoryBytes']} </font><br align="center" /><font color="red">Max Memory Used: {n['maxMemoryBytes']} </font><br align="center" /><font color="red">Selectivity: 0.00%% </font><br align="center" /></td><td VALIGN="BOTTOM"><font point-size="14"> <b>Output</b></font><br align="right" />{n['netOutputRowRate']} rows/sec,  N/A<br align="right" />{n['netOutputRate']} B/sec,  N/A<br align="right" />Rowtime:  N/A<br align="right" /></td></tr></table>            

            # define edges
            if n['numInputNodes'] > 0:
                incoming = n['inputNodes'].split(' ')
                for i in incoming:
                    k = ''' "%s" -> "%s";''' % (i, n['nodeId'])
                    if not k in edges:
                        edges[k] = None
                        dot.append(k)
            # define edges
            if n['numOutputNodes'] > 0:
                outgoing = n['outputNodes'].split(' ')
                for o in outgoing:
                    k = ''' "%s" -> "%s";''' % (n['nodeId'], o)
                    if not k in edges:
                        edges[k] = None
                        dot.append(k)
        
 #           dot.append("    }")
    dot.append("}")

st.sidebar.write('# Telemetry')
st.sidebar.write('## Parameters')

dtab, ctab, jtab, gtab, ltab = st.tabs(["Data", "Chart", "JSON", "Graphviz", "Logs"])

filename = st.sidebar.file_uploader('Choose the file to upload', type='json')
if filename is not None:
    bytes_data = filename.getvalue()
    tgraph = json.loads(bytes_data.decode('utf-8'))

    with ltab:
        st.text('This tab is used for debug')
        st.write('Selected client file `%s`' % filename)

        st.write('## Telemetry file: ')
        st.write(filename)

    with jtab:
        st.json(tgraph, expanded=False)

    with dtab:
        st.write("### Server Info")
        server_df = json_normalize(tgraph, max_level=0)
        st.dataframe(server_df)

    sessions = []

    deadGraphs = st.sidebar.checkbox('Show dead graphs',value=False)
    withProxies = st.sidebar.checkbox('Include proxy nodes', value=False)

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


    with dtab:
        st.write("### Stream Graphs")
        st.dataframe(graph_list)
        
    node_list = []

    dot = ['''
digraph {
graph [pad="0.25", ranksep = "1.0", nodesep="1.0"];
edge [color=gray, penwidth=2]; 
''']
# rankdir="LR";
    for session in tgraph['sessions']:
        if session['sessionName'] == sess_name:
            session_to_dot(session, withProxies, dot, node_list)

    dot.append('}')
    dot_string = "\n".join(dot)

    with dtab:
        st.write("### Stream Operators (Nodes)")
        st.dataframe(node_list)

    with gtab:
        st.write('### Telemetry Chart Dot file')
        st.code(dot_string)

    with ctab:
        st.write('### Telemetry Chart')
        st.graphviz_chart(dot_string)    

    with ltab:
        for n in node_list:
            st.code(str(n))