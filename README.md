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

* parser.py

STANDALONE SCRIPT:

```

DESCRIPTION
This is a parser with:
input:
a csv/csv.gz data file dumped from ORACLE DB. (dump file contains extra spaces, newlines, etc.)
a schema file
output:
a list of dictionaries, each of which represents the parsed result of each data example against the schema
a csv file with schema as columns, dictionaries as rows, and TAB as deliminator within each row.

it is a stand-alone python script, and run by
python parser.py --fin=cms_conf.csv.gz --schema=schema -fout=cms_conf_parsed.csv
Its options allow us to specify input data file, input schema file.
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

we may re-use it later in other program via import statement.

```

FUNCTIONS

```

    date_cvt(date_str)
        convert a date string (input) to a datetime.date object (output)
    
    parse_dataframe_by_match_record(fdataframe, attribute2type)
        parse dataframe, by specifying each record and reach field
        input: a dataframe file, and an ordered dict of each schema attribute and its type
        output: a list of dicts, each of which is the parsed result of each conference by the schema
        allow PRES_TITLE field span more than one lines, and assume other fields can't span more than one line
    
    parse_dataframe_by_split(fdataframe, attribute2type)
        Parse a dataframe file, by specifying record separator and file separator and splitting according to the separtors
        input: a dataframe file, and an ordered dict of each schema attribute and its type
        output: a list of dicts, each of which is the parsed result of each conference by the schema
        it assumes that each field can't span more than one lines.
    
    parse_schema(fschema)
        parse a schema file for the type of each field
        input: a schema file
        output: an ordered dictionary of each schema attribute and its type
    
    type_db2py(dbtype)
        convert from db types to python types
        input: a string which represents a db type
        output: a python type
```

TODO
------------
