CREATE TABLE branch_traces (
    branch_addr STRING,
    branch_type STRING,
    taken STRING,
    target STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/warehouse/branch_traces/branch_traces.csv' INTO TABLE branch_traces;

