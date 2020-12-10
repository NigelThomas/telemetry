-- run this query to identify blocked/blocking nodes 

SELECT ovf.node_id as blocked_node_id, ovf.sched_state as blocked_state
     , und.node_id as blocking_node_id, und.sched_state as blocking_state
     , und.source_sql blocking_sql
FROM (
    SELECT * from TABLE(getStreamOperatorInfo(0,1)) 
    WHERE last_exec_result = 'UND'
) und
JOIN (
    SELECT * FROM TABLE(getStreamOperatorInfo(0,1))
    WHERE last_exec_result = 'OVF'
    ) ovf
ON ','||ovf.output_nodes||',' LIKE '%,'||cast(und.node_id AS VARCHAR(5))||',*' 
;


