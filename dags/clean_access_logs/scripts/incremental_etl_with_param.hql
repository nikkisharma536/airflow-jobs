-- Creating a ETL script by passing run_date value

-- HIVE CONF
set hive.exec.dynamic.partition.mode=nonstrict;

-- external parameters
-- set RUN_DATE=2019-01-05;
-- USAGE:
-- hive -hiveconf RUN_DATE='2019-01-05' -f script.hql


CREATE SCHEMA IF NOT EXISTS nik;
CREATE SCHEMA IF NOT EXISTS prod;

CREATE EXTERNAL TABLE IF NOT EXISTS prod.clean_access_log(
    ip STRING,
    request_type STRING,
	uri STRING,
	protocol STRING,
    bytes_sent STRING,
	referer STRING,
	useragent STRING
)
PARTITIONED BY (dte STRING)
STORED AS PARQUET
LOCATION 's3://nikita-ds-playground/prod/clean_access_log/';

DROP TABLE nik.access_log_temp;

CREATE EXTERNAL TABLE IF NOT EXISTS nik.access_log_temp
(log STRING)
LOCATION 's3://nikita-ds-playground/raw/access-log/${hiveconf:RUN_DATE}/';


INSERT OVERWRITE TABLE prod.clean_access_log PARTITION(dte)
select
split(log, " ")[0] as ip,
substr(split(log, " ")[5], 2) as request_type,
split(log, " ")[6] as uri,
cast(split(log, " ")[8] as int) as protocol,
cast(split(log, " ")[9] as bigint) as bytes_sent,
substr( split(log, " ")[10],2, length( split(log, " ")[10])-2 ) as referrer,
substr(split(log, "\\\" \\\"")[1], 1, length(split(log, "\\\" \\\"")[1]) - 1 ) as useragent,
'${hiveconf:RUN_DATE}' as dte
from nik.access_log_temp ;

DROP TABLE nik.access_log_temp;

-- verify data
-- select dte, count(1) from prod.clean_access_log group by dte;
