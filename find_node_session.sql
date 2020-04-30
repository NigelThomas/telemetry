-- Example of how to find out more about a node if it appears in an error message
--

select g.graph_id, op.node_id, op.name_in_query_plan, g.session_id, g.source_sql
 , s.session_name
from table(sys_boot.mgmt.getStreamOperatorInfo(0,0)) op
JOIN table(sys_boot.mgmt.getStreamGraphInfo(0,0)) g ON g.graph_id = op.graph_id
JOIN sys_boot.mgmt.sessions_view s  on s.ID = g.session_id
where op.graph_id < 1000
and name_in_query_plan like 'AspenCalcRel.#29150:31979%'
;
