S3_DB.STAGE_SCHEMA.MEMBERS_DETAILS_S3DROP DATABASE IF EXISTS S3_db;
create or replace database S3_db;

CREATE or REPLACE table S3_DB.public.members_details(
name String, 
province String,	
constituency String,
party String,
oath_taking_date DATE,	
city String,	
constituency_type String,
country String
);

CREATE or REPLACE table S3_DB.public.sessions(
constituency String,
name String,
date date 
);

select *
from members_details
limit 10;


CREATE SCHEMA file_format_schema;
CREATE OR REPLACE file format S3_DB.file_format_schema.format_csv
    type = 'CSV'
    field_delimiter = ','
    RECORD_DELIMITER = '\n'
    skip_header =1
;    

CREATE SCHEMA stage_schema;

CREATE or REPLACE STAGE stage_schema.members_details_s3
    url = "s3://members-details-scraped-cleaned/"
    credentials = (aws_key_id = '############'
    aws_secret_key= '################################')
    FILE_FORMAT = S3_DB.file_format_schema.format_csv
;

CREATE or REPLACE STAGE stage_schema.sessions_s3
    url = "s3://members-attendence-transformed/"
    credentials = (aws_key_id = 'AKIA24PJHA677MKJN3H6'
    aws_secret_key= 'tQAXiygXqYX8qA2cAPNnWG39cmlUrbTZ/DWIsyZ8')
    FILE_FORMAT = S3_DB.file_format_schema.format_csv
;

list @S3_DB.stage_schema.members_details_s3;
list @S3_DB.stage_schema.sessions_s3;


CREATE OR REPLACE SCHEMA S3_DB.snowpipe_schema;


CREATE OR REPLACE PIPE S3_DB.snowpipe_schema.members_details_snowpipe
auto_ingest = True
AS 
COPY INTO S3_DB.PUBLIC.MEMBERS_DETAILS
from @S3_DB.stage_schema.members_details_s3;


CREATE OR REPLACE PIPE S3_DB.snowpipe_schema.sessions_snowpipe
auto_ingest = True
AS 
COPY INTO S3_DB.PUBLIC.sessions
from @S3_DB.stage_schema.sessions_s3;


DESC PIPE S3_DB.snowpipe_schema.members_details_snowpipe;
DESC PIPE S3_DB.snowpipe_schema.sessions_snowpipe;


ALTER TABLE S3_DB.PUBLIC.MEMBERS_DETAILS ADD PRIMARY KEY (Name,Constituency);
ALTER TABLE S3_DB.PUBLIC.SESSIONS ADD PRIMARY KEY (Name,Constituency);


select *
from members_details
limit 10;

select *
from SESSIONS
order by name desc,date desc
limit 10;
