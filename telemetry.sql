
!outputformat vertical
create or replace schema telemetryStuff;
set schema 'telemetryStuff';
set path 'telemetryStuff';
create or replace function noZero(c bigint)
returns varchar(32)
contains sql
deterministic
return case when c = 0 then '?' else cast(c as varchar(32)) end;

create or replace function regexReplace(s varchar(8192), orig varchar(256), replacement varchar(256))
returns varchar(10000)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME 'class com.sqlstream.aspen.util.Util.regex_replace';

SELECT * FROM (VALUES('digraph {')) AS v("   ");

SELECT 
    CASE WHEN 
        NAME_IN_QUERY_PLAN NOT LIKE '%pro' AND
        NAME_IN_QUERY_PLAN NOT LIKE '$Proxy%'
         THEN
    '"' || NODE_ID || '"' || '[penwidth=3.0,style="bold,filled",fillcolor=' ||
    CASE WHEN LAST_EXEC_RESULT = 'UND'
         THEN 'green'
         WHEN LAST_EXEC_RESULT = 'OVR'
         THEN 'red'
         WHEN LAST_EXEC_RESULT = 'YLD'
         THEN 'yellow'
         WHEN LAST_EXEC_RESULT = 'EOS'
         THEN 'blue'
         ELSE 'white'
         END ||
         ',tooltip="' || regexReplace('\"', query_plan, '\\"') || '"' ||
         ', label=' ||
    '<Input: ' || noZero(NET_INPUT_ROWS) || ' rows @ ' ||
    CAST(COALESCE(INPUT_ROWTIME_CLOCK, CURRENT_TIMESTAMP) AS VARCHAR(32)) ||
    ', ' || CAST(NET_INPUT_BYTES AS VARCHAR(32)) || ' Bytes' ||
    '<br /> <br />' ||
    '<B>' || NAME_IN_QUERY_PLAN || '<br />' || LAST_EXEC_RESULT || '</B>' ||
    '<br /> <br />' ||
    'Output: ' || noZero(NET_OUTPUT_ROWS) || ' rows @' ||
    CAST(COALESCE(OUTPUT_ROWTIME_CLOCK, CURRENT_TIMESTAMP) AS VARCHAR(32)) ||
    ', ' || CAST(NET_OUTPUT_BYTES AS VARCHAR(32)) || ' Bytes' ||
    '>];' ELSE '' END AS "  ",
    CASE WHEN INPUT_NODES IS NULL
         THEN NULL
         WHEN POSITION(' ' IN INPUT_NODES) = 0
         THEN '"' ||
              INPUT_NODES ||
              '"'  || ' -> ' || '"' ||
              NODE_ID || '";'
         ELSE '"' || 
              SUBSTRING(INPUT_NODES FROM 1 FOR POSITION(' ' IN INPUT_NODES) - 1) ||
              '"'  || ' -> ' || '"' ||
              NODE_ID || '";'
         END AS "    ",
   CASE WHEN POSITION(' ' IN INPUT_NODES) > 0
        THEN '"' ||
              SUBSTRING(INPUT_NODES, POSITION(' ' IN INPUT_NODES) + 1) ||
              '"'  || ' -> ' || '"' ||
              NODE_ID || '";'
         ELSE NULL
         END AS "        "
FROM TABLE(sys_boot.mgmt.getStreamOperatorInfo(0, 0)) WHERE LAST_EXEC_RESULT <> 'EOS' AND (NAME_IN_QUERY_PLAN NOT LIKE 'StreamSinkPortRel%' AND NAME_IN_QUERY_PLAN NOT LIKE 'NetworkRel%')
-- and node_id in ('1423.2','1421.0','1422.0')
;

SELECT * FROM (VALUES('}')) AS v("   ");
