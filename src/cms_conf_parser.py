#!/usr/bin/env python

"""
This is a stand-alone python script, and run by
python src/cms_conf_parser.py --fdat data/cms_conf.csv.gz --fsch data/schema -fpsd data/cms_conf_parsed.csv -fccw data/cms_conf_ct_perweek.csv
input:
data/cms_conf.csv.gz: a csv/csv.gz data file dumped from ORACLE DB. (dump file contains extra spaces, newlines, etc.)
data/schema: a schema file decribing the attributes of each conference record (see below)
output:
data/cms_conf_parsed.csv: a csv file with the schema as columns, conference records as rows, with the attributes in each record delimited by TAB
data/cms_conf_ct_perweek.csv: a csv file with week and conf ct as columns, each record reprepsenting the week and the nb of conferences in the week, which delimited by TAB

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
"""

import argparse
import datetime
import re
import gzip
import collections
import csv


def type_db2py(dbtype):
    """ convert from db types to python types
    input: 
    dbtype: a string which represents a db type
    output: 
    pytype: a python type
    """

    types_dict = {
        'NOT NULL NUMBER': int,
        'VARCHAR2\(\d+\)': str,
        'DATE': date_cvt
            }

    pytype = None 
    for key in types_dict.keys():
        if re.search(key, dbtype):
            pytype = types_dict[key]
            break

    return pytype


def date_cvt(date_str):
    """ convert a date string to a datetime.date object 
    input: 
    date_str: a string represnting a date
    output:
    a datatime.date object
    """
    # year
    dmy = date_str.split('-')
    if int(dmy[2]) <= 50:
        dmy[2] = 2000 + int(dmy[2])
    else:
        dmy[2] = 1900 + int(dmy[2])

    # month
    mon={'JAN':1, 'FEB':2, 'MAR':3, 'APR':4, 'MAY':5, 'JUN':6, 'JUL':7, 'AUG':8, 'SEP':9, 'OCT':10, 'NOV':11, 'DEC':12}

    return datetime.date(dmy[2], mon[dmy[1]], int(dmy[0]))



    
def parse_schema(fschema):
    """parse a schema file for the type of each field
    input: 
    fschema: a schema file
    output: 
    attribute2type: an ordered dictionary of each schema attribute and its type
    """
    # schema
    lines = open(fschema).readlines()
    attribute2type = collections.OrderedDict()
    for line in lines:
        pair = line.split(None, 1)
        # print line, pair
        attribute2type[pair[0]] = type_db2py(pair[1])
    # print attribute2type.items()
    return  attribute2type


def parse_dataframe_by_split(fdataframe, attribute2type):
    """Parse a dataframe file, by specifying record separator and file separator and splitting according to the separtors
    it assumes that each field can't span more than one lines.
    input: 
    fdataframe: a dataframe file, 
    attribute2type: an ordered dict of each schema attribute and its type
    output: 
    conf_list: a list of dicts, each of which is the parsed result of each conference by the schema
    """    
    schema = attribute2type.keys()

    # dataframe
    lines = gzip.open(fdataframe).readlines()

    # preprocessing
    lines = lines[2:-5] # remove preceding and trailing SQL lines
    text = ''.join(lines)
    while text[-1]=='\n': # remove trailing new line chars, otherwise splitting the last conf into conf items will have empty item
        text = text[0:-1]

    # split into conferences
    confs = text.rsplit('\n\n') # choose rsplit not split because the first attribute in the schema must not be None, while the last attribute can be.

    # parse each conference by schema
    field_separator = '(?<=\d),  +| ,(?! )|(?<=\D-\d\d),|\n' # the first alternative separtor is for not separting "State, US", and the second alternative separator is for separating "CONF_START,CONF_CATEGORY"

    confs_list = []
    j_start = 0 # for dealing with missing value e.g. all (?) missing values happen for CONF_WEB
    for i in range(0, len(confs)):
        
        conf = confs[i]
        conf_items = re.split(field_separator, conf)

        assert(len(conf_items) <= len(schema)) # disallow more conf items than schema attributes. allow fewer conf items than schema attributes, because of missing value and the way I split into conferences; 
        if j_start == 0:
            conf_dict =collections.OrderedDict()
        for j in range(0, len(conf_items)):
            conf_dict[schema[j+j_start]] = attribute2type[schema[j+j_start]](conf_items[j])  # convert the conf item to the attribute's type

        if j+j_start+1 < len(schema): # where there is missing value for some attribute
            conf_dict[schema[j+j_start+1]] = None
            j_start = j+j_start+2  # update j_start
            if j_start == len(schema):
                j_start = 0
        elif j+j_start+1 == len(schema):
            j_start = 0
        else:  # unlikely
            raw_input('Error! enter <return> to continue .. ')
            import sys
            sys.exit()
        
        if j_start ==0:
            confs_list.append(conf_dict)

        
    return confs_list        
        


def parse_dataframe_by_match_record(fdataframe, attribute2type):
    """  parse dataframe, by specifying each record and reach field
    allow PRES_TITLE field span more than one lines, and assume other fields can't span more than one line
    input: 
    fdataframe: a dataframe file, 
    attribte2type: an ordered dict of each schema attribute and its type
    output: 
    conf_list: a list of dicts, each of which is the parsed result of each conference by the schema
    """

    schema = attribute2type.keys()

    # dataframe
    text = gzip.open(fdataframe).read()

    # parse the conference records in dataframe file
    
    conf_pattern = re.compile(r'''  # a regex that match a conference record
    ^(.{10}),(.{10})$\n
    ^(.*)$\n
    ^(.{100}),(.{9}),(.{,8})$\n
    ^(.*)$\n
    ^(.*)$\n
    ^(.*)$\n
    ^(.*)$\n
    ^([\s\S]*?)\n                  # a value of the field PRES_TITLE may span more than one lines, while other fields don't
    ^(.{,8})$\n
    ^(.*)$
    ''', re.VERBOSE | re.M)
    
    matches = re.finditer(conf_pattern, text)
    confs_list = []
    for match in matches:
        confs_list.append(collections.OrderedDict(zip(schema, match.groups())))


    # convert the values in the dictionaries to correct types
    confs_list = [collections.OrderedDict((k, attribute2type[k](v.strip())) for (k, v) in l.iteritems()) for l in confs_list]
    
    return confs_list        
        

def group_confs_by_week(confs_list):
    ''' Group the confs by week
    Input: 
    confs_list: a list of dictionaries, each represents a conf
    output: 
    grouped: a dict of (week, list of confs), 
    '''

    grouped = collections.OrderedDict()
    for conf in confs_list:
        yearweek = conf['CONF_START'].isocalendar()[0:2]
        # print yearweek, type(yearweek)
        grouped.setdefault(yearweek, []).append(conf)
    return grouped


def count_confs_by_week(grouped):
    ''' Count the confs by week
    Input: 
    grouped: a dict of (week, list of confs), 
    output: 
    confct_by_wk: a list of [week, conf ct]
    '''
    
    startyearweek = grouped.keys()[0]
    endyearweek = grouped.keys()[-1]
    confct_by_wk = []
    # for week in range(startyearweek[1], datetime.date(startyearweek[0], 12, 28).isocalendar()[1]+1): # assume the conferences are given starting from the earlies week when there is a conference.
    for week in range(1, datetime.date(startyearweek[0], 12, 28).isocalendar()[1]+1):      # assume the conferences are given starting from the first week of a year
        confct_by_wk.append([(startyearweek[0], week), len(grouped.setdefault((startyearweek[0], week), []))])
    for year in range(startyearweek[0]+1, endyearweek[0]):
        for week in range(1, datetime.date(year, 12, 28).isocalendar()[1]+1):
            confct_by_wk.append([(year, week), len(grouped.setdefault((year, week), []))])
    for week in range(1, endyearweek[1]+1):
        confct_by_wk.append([(endyearweek[0], week), len(grouped.setdefault((endyearweek[0], week), []))])

    return confct_by_wk


def count_confs_in_future(confct_by_wk, periods):
    ''' for each week, count the nb of confs in the next period[p] weeks
    input:
    confct_by_wk: nb of confs in each week
    periods: nb of future weeks
    output:
    confct_future: nb of confs in future weeks, from each week
    '''
    confct_future = [[x[0]] for x in confct_by_wk]
    # print confct_future
    for p in periods:
        for i in range(0,len(confct_by_wk)-p):
            confct_future[i].append(sum(x[1] for x in confct_by_wk[i + 1: i + p + 1]))
        for i in range(len(confct_by_wk)-p, len(confct_by_wk)):
            confct_future[i].append(None)
    return confct_future


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parse some dataframe.')
    parser.add_argument('--fdat', dest='fdat', help='input dataframe file')
    parser.add_argument('--fsch', dest='fsch', help='input schema file')
    parser.add_argument('--fpsd', dest='fpsd', help='output parsed data csv file')
    parser.add_argument('--fccw', dest='fccw', help='output conf-ct-per-week csv file')
    parser.add_argument('--fccf', dest='fccf', help='output conf-ct-in-future-from-each-week csv file')            
    args = parser.parse_args()


    # 1. parse the data frame into list of conf dicts
    
    attribute2type = parse_schema(args.fsch)

    # two ways of parsing dataframe: (prefer the second way over the first)
    # by specifying separtors for separating records and fields, assume each field of a record only spans one line
    # confs_list1 = parse_dataframe_by_split(args.fdat, attribute2type)
    # by matching each record and field.  allow PRES_TITLE field span more than one lines, and assume other fields can't span more than one line
    confs_list = parse_dataframe_by_match_record(args.fdat, attribute2type)    

    # Output the parsed result

    # print len(confs_list1)
    print "There are {:d} conference records".format(len(confs_list))

    # print "*******"
    # for conf in confs_list2:
    #     print conf
    #     print "*******"

    schema = attribute2type.keys()
    confs_list = sorted(confs_list, key=lambda k: k['CONF_START'])  # sort confs by date, for later grouping by week
    with open(args.fpsd, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=schema,  delimiter='\t')
        writer.writeheader()
        writer.writerows(confs_list)


    # 2. group confs by week
    grouped = group_confs_by_week(confs_list)

    # # output the grouped result
    # print "*********"
    # for week,confs in grouped:
    #     print "For week " + str(week) + ":"
    #     print confs
    #     print "**********"

    # 3. count confs by week
    confct_by_wk = count_confs_by_week(grouped)
    
    # # output the conf ct per week
    # print "**************"
    # for week, ct in confct_by_wk:
    #     print week, ct

    with open(args.fccw, 'w') as csvfile:
        csvfile.write('week\tconfct\n')
        for week, ct in confct_by_wk:
            csvfile.write('{}\t{}\n'.format(week, ct))

    # 4. count confs in certain nb of future weeks, from each week
    periods=[1,4,12] # for next week, next month, and next 3 months
    confct_future = count_confs_in_future(confct_by_wk, periods)
        
    # print "***************"
    # for item in confct_future:
    #     print item

    with open(args.fccf, 'w') as csvfile:
        csvfile.write('week\tconfct in 1wk\tconfct in 4wks\tconfct in 12wks\n')
        for week, ct1, ct4, ct12 in confct_future:
            csvfile.write('{}\t{}\t{}\t{}\n'.format(week, ct1, ct4, ct12))
