Mining Data from Calendar
=============

Description
-------------


In this project, we would like to extract some information from physicists' calendars, which will be used for predicting the datasets used in their near future research.

Physicists' calendars contain research conferences which they have attended or plan to. Past calendar information can be used for building statistical models, and future calendar information can be used as predictors for prediction.

Design
-------------

The canlendar data will probably be provided as a (relational?) database from CERN.

In the first step, we will extract from physicists' calendars the number of conferences in each time slot (e.g. per week or month) (and ...?).
Different conferences may have different numbers of talks, and we will assign a bigger weight to a conference which has more talks.
Q:
We don't extract different information from calendars for different datasets?
Are we to study the relation between calendars and all the datasets (not individual dataset)?

In the second step, we will model the relation between the dataset access by physicists and their calendar information over the time. My tentative plan is to choose an appropriate (multivariate) time series or online learning model.

Tools
-------------

I plan to program mainly in Python. To perform queries on the given database, I may use some database driver module.

For statistical modelling the data, I would like to program in R (or Python), to utilize R's statistical and machine learning packages (or NumPy, SciPy, sklearn modules in Python)

For gluing scripts and commands together, I plan to use bash script (or Python).

Scripts
-------------

* cms_conf_parser.py


```
This is a stand-alone python script, and run by
python src/cms_conf_parser.py --fdat data/cms_conf.csv.gz --fsch data/schema --fpsd data/cms_conf_parsed.csv --fccw data/cms_conf_ct_perweek.csv --fccf dat
a/cms_conf_ct_future.csv
input:
data/cms_conf.csv.gz: a csv/csv.gz data file dumped from ORACLE DB. (dump file contains extra spaces, newlines, etc.)
data/schema: a schema file decribing the attributes of each conference record (see below)
output:
data/cms_conf_parsed.csv: a csv file with the schema as columns, conference records as rows, with the attributes in each record delimited by TAB
data/cms_conf_ct_perweek.csv: a csv file with week and conf ct as columns, each record reprepsenting the week and the nb of conferences in the week, which
delimited by TAB
data/cms_conf_ct_future.csv: a csv file with week, conf ct in future 1 week, conf ct in future 4 weeks, and conf ct in future 12 weeks.

e.g.
For CMS calendar data, the schema file is:
CONF_ID                        NOT NULL NUMBER
PRES_ID                        NOT NULL NUMBER
CONF_NAME                               VARCHAR2(1024)
CONF_NAME_SHORT                         VARCHAR2(100)
CONF_START                              DATE
CONF_CATEGORY                           VARCHAR2(8)
CONF_DESCRIPTION_CATEGORY               VARCHAR2(1024)
CONF_CITY                               VARCHAR2(1024)
COUNTRY                                 VARCHAR2(1024)
CONF_WEB                                VARCHAR2(1024)
PRES_TITLE                              VARCHAR2(1024)
PRES_CATEGORY                           VARCHAR2(8)
PRES_DESCRIPTION_CATEGORY               VARCHAR2(1024)

This is also a module with functions maybe reused in other program via import statement.

count_confs_by_week(grouped)
Count the confs by week
Input:
grouped: a dict of (week, list of confs),
output:
confct_by_wk: a list of [week, conf ct]

count_confs_in_future(confct_by_wk, periods)
for each week, count the nb of confs in the next period[p] weeks
input:
confct_by_wk: nb of confs in each week
periods: nb of future weeks
output:
confct_future: nb of confs in future weeks, from each week

date_cvt(date_str)
convert a date string to a datetime.date object
input:
date_str: a string represnting a date
output:
a datatime.date object

group_confs_by_week(confs_list)
Group the confs by week
Input:
confs_list: a list of dictionaries, each represents a conf
output:
grouped: a dict of (week, list of confs),

parse_dataframe_by_match_record(fdataframe, attribute2type)
parse dataframe, by specifying each record and reach field
allow PRES_TITLE field span more than one lines, and assume other fields can't span more than one line
input:
fdataframe: a dataframe file,
attribte2type: an ordered dict of each schema attribute and its type
output:
conf_list: a list of dicts, each of which is the parsed result of each conference by the schema

parse_dataframe_by_split(fdataframe, attribute2type)
Parse a dataframe file, by specifying record separator and file separator and splitting according to the separtors
it assumes that each field can't span more than one lines.
input:
fdataframe: a dataframe file,
attribute2type: an ordered dict of each schema attribute and its type
output:
conf_list: a list of dicts, each of which is the parsed result of each conference by the schema

parse_schema(fschema)
parse a schema file for the type of each field
input:
fschema: a schema file
output:
attribute2type: an ordered dictionary of each schema attribute and its type

type_db2py(dbtype)
convert from db types to python types
input:
dbtype: a string which represents a db type
output:
pytype: a python type


```

TODO
------------
