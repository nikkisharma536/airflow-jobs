
-- Summary table having URI and dte

-- HIVE CONF
set hive.exec.dynamic.partition.mode=nonstrict;

-- external parameters
-- set RUN_DATE=2019-01-05;
-- USAGE:
-- hive -hiveconf RUN_DATE='2019-01-05' -f summary.hql

CREATE EXTERNAL TABLE IF NOT EXISTS prod.summary_access_log(
    uri STRING,
    count INT)
    PARTITIONED BY (dte STRING)
STORED AS PARQUET
LOCATION 's3://nikita-ds-playground/prod/summary_access_log/';


INSERT OVERWRITE TABLE prod.summary_access_log PARTITION(dte)
    SELECT
    uri,
    COUNT(uri) as count,
    dte
    FROM prod.clean_access_log
    WHERE dte = '${hiveconf:RUN_DATE}'
    GROUP BY dte,uri ;

