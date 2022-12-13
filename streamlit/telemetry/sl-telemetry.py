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

import telemetry_functions as tf


st.sidebar.write('# Telemetry')
st.sidebar.write('## Parameters')

dtab, ctab, jtab, gtab, ltab = st.tabs(["Data", "Chart", "JSON", "Graphviz", "Logs"])

filename = st.sidebar.file_uploader('Choose the file to upload', type='json')
if filename is not None:
    bytes_data = filename.getvalue()
    telemetry_tree = json.loads(bytes_data.decode('utf-8'))

    with ltab:
        st.text('This tab is used for debug')
        st.write('Selected client file `%s`' % filename)

        st.write('## Telemetry file: ')
        st.write(filename)

    with jtab:
        st.json(telemetry_tree, expanded=False)

    with dtab:
        st.write("### Server Info")
        server_df = json_normalize(telemetry_tree, max_level=0)
        st.dataframe(server_df)


    deadGraphs = st.sidebar.checkbox('Show dead graphs',value=False)
    withProxies = st.sidebar.checkbox('Include proxy nodes', value=False)

    transformed_tree = tf.transform_tree(telemetry_tree, deadGraphs, withProxies)

    sess_name = st.sidebar.selectbox('Choose a session to examine:',transformed_tree["sessionNames"])


    with dtab:
        st.write("### Stream Graphs (all of them for now)")
        st.dataframe(transformed_tree['graphs'])
        
    dot = ['''
digraph {
graph [pad="0.25", ranksep = "1.0", nodesep="1.0"];
edge [color=gray, penwidth=2]; 
''']
# rankdir="LR";
    for session in transformed_tree['sessions']:
        if session['sessionInfo']['sessionName'] == sess_name:
            with ltab:
                st.write(f"### Session {sess_name}")
                st.text(session)

            tf.session_to_dot(session, withProxies, dot)

    dot.append('}')
    dot_string = "\n".join(dot)

    node_list = transformed_tree['nodes']

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
        st.write("## Transformed Tree")
        st.json(transformed_tree)