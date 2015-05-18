#!/usr/bin/env python

"""
This is a stand-alone python script, and run from src directory
python src/cms_conf_parser.py --indump data/conf/cms_conf.csv.gz --inschema data/conf/schema --outdir data/conf
input:
data/cms_conf.csv.gz: a csv.gz data file dumped from ORACLE DB. (dump file contains extra spaces, newlines, etc.)
data/schema: a schema file decribing the attributes of each conference record (see below)
output:
data/cms_conf_parsed.csv.gz: a csv.gz file with the schema as columns, conference records as rows (sorted by date), with the attributes in each record delimited by TAB. I.e. reorganize cms_conf.csv.gz in a cleaner way.
data/cms_conf_ct_perweek.csv.gz: a csv.gz file with week and conf ct as columns, each record reprepsenting the week and the nb of conferences in the week, which delimited by TAB
data/cms_conf_ct_future.csv.gz: a csv.gz file with week, conf ct in future 1 week, conf ct in future 4 weeks, and conf ct in future 12 weeks. 

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
import ordereddict

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
    # attribute2type = collections.OrderedDict()
    attribute2type = ordereddict.OrderedDict()
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
            # conf_dict =collections.OrderedDict()
            conf_dict =ordereddict.OrderedDict()
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
    fdataframe: a dataframe file of cvs.gz format, 
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
        # confs_list.append(collections.OrderedDict(zip(schema, match.groups())))
        confs_list.append(ordereddict.OrderedDict(zip(schema, match.groups())))


    # convert the values in the dictionaries to correct types
    confs_list = [ordereddict.OrderedDict((k, attribute2type[k](v.strip())) for (k, v) in l.iteritems()) for l in confs_list]
    
    return confs_list        
        

########### group and count confs by calendar weeks

def iso_year_start(iso_year):
    '''The gregorian calendar date of the first day of the given ISO year'''
    fourth_jan = datetime.date(iso_year, 1, 4)
    delta = datetime.timedelta(fourth_jan.isoweekday()-1)
    return fourth_jan - delta

def iso_to_gregorian(iso_year, iso_week, iso_day):
    '''Gregorian calendar date for the given ISO year, week and day'''
    year_start = iso_year_start(iso_year)
    return year_start + datetime.timedelta(days=iso_day-1, weeks=iso_week-1)


def group_confs_by_week(confs_list):
    ''' Group the confs by calendar week
    Input: 
    confs_list: a list of dictionaries, each represents a conf
    output: 
    grouped: a dict of (week, list of confs), 
    '''

    # grouped = collections.OrderedDict()
    grouped = ordereddict.OrderedDict()
    for conf in confs_list:
        yearweek = conf['CONF_START'].isocalendar()[0:2]
        # print yearweek, type(yearweek)
        grouped.setdefault(yearweek, []).append(conf)
    return grouped


def count_confs_by_week(grouped):
    ''' Count the confs by calendar week
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




########### group and count confs by self-defined weeks, where the first week in a year starts from jan 1 and the last week has more than 7 days to include Dec 31 (and 30)

def mycalendar(date):
    ''' convert from Gregorian Calendar date to (year, week, day) starting from Jan 1
    '''
    year = date.year
    delta = date - datetime.date(date.year, 1, 1)
    week = delta.days / 7 + 1
    day = delta.days % 7 + 1
    if week == 53: # when date is the last one or two days where there are less than 7 days left to form a week
        week -= 1
        day += 7
    return (year,week,day)

def mine_to_gregorian(my_year, my_week, my_day):
    '''Gregorian calendar date for the given year, week and day starting from Jan 1'''
    year_start = datetime.date(my_year, 1, 1)
    return year_start + datetime.timedelta(days=my_day-1, weeks=my_week-1)


def group_confs_by_myweek(confs_list):
    ''' Group the confs by self-defined week
    Input: 
    confs_list: a list of dictionaries, each represents a conf
    output: 
    grouped: a dict of (week, list of confs), 
    '''

    # grouped = collections.OrderedDict()
    grouped = ordereddict.OrderedDict()
    for conf in confs_list:
        yearweek = mycalendar(conf['CONF_START'])[0:2]
        # print yearweek, type(yearweek)
        grouped.setdefault(yearweek, []).append(conf)
    return grouped


def count_confs_by_myweek(grouped):
    ''' Count the confs by self-def week
    Input: 
    grouped: a dict of (week, list of confs), 
    output: 
    confct_by_wk: a list of [week, conf ct]
    '''
    
    startyearweek = grouped.keys()[0]
    endyearweek = grouped.keys()[-1]
    confct_by_wk = []
    # for week in range(startyearweek[1], datetime.date(startyearweek[0], 12, 28).isocalendar()[1]+1): # assume the conferences are given starting from the earlies week when there is a conference.
    for week in range(1, mycalendar(datetime.date(startyearweek[0], 12, 28))[1]+1):      # assume the conferences are given starting from the first week of a year
        confct_by_wk.append([(startyearweek[0], week), len(grouped.setdefault((startyearweek[0], week), []))])
    for year in range(startyearweek[0]+1, endyearweek[0]):
        for week in range(1, mycalendar(datetime.date(year, 12, 28))[1]+1):
            confct_by_wk.append([(year, week), len(grouped.setdefault((year, week), []))])
    for week in range(1, endyearweek[1]+1):
        confct_by_wk.append([(endyearweek[0], week), len(grouped.setdefault((endyearweek[0], week), []))])

    return confct_by_wk


################ count confs in future weeks, independent of def of a week

def count_confs_in_future(confct_by_wk, periods):
    ''' for each week, count the nb of confs in the next period[p] weeks
    input:
    confct_by_wk: a list of conf nb in each week, a list of [week, conf ct]
    periods: a list of future periods' lengths in weeks. a list of [period 1 lenth, period 2 length, ...]
    output:
    confct_future: a list of conf nb in future weeks, from each week. a list of [week, conf ct in period 1, conf ct in period 2, ...]
    '''
    confct_future = [[x[0]] for x in confct_by_wk]
    # print confct_future
    for p in periods:
        for i in range(0,len(confct_by_wk)-p):
            confct_future[i].append(sum(x[1] for x in confct_by_wk[i + 1: i + p + 1]))
        for i in range(len(confct_by_wk)-p, len(confct_by_wk)):
            confct_future[i].append(None)
    return confct_future


###################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parses the conference data dump file into a conference count time series.')
    parser.add_argument('--indump', dest='indump', help='a csv.gz file for conference data dump from a database')
    parser.add_argument('--inschema', dest='inschema', help='a plain text file for the schema of the conference data dump file')
    parser.add_argument('--outdir', dest='outdir', help='a dir for csv.gz files for conference count per week time series, for conference count for future weeks, and for parsed conference records')
    args = parser.parse_args()


    # 1. parse the data frame into list of conf dicts
    
    attribute2type = parse_schema(args.inschema)
    # print attribute2type

    # two ways of parsing dataframe: (prefer the second way over the first)
    # by specifying separtors for separating records and fields, assume each field of a record only spans one line
    # confs_list1 = parse_dataframe_by_split(args.indump, attribute2type)
    # by matching each record and field.  allow PRES_TITLE field span more than one lines, and assume other fields can't span more than one line
    confs_list = parse_dataframe_by_match_record(args.indump, attribute2type)    

    # Output the parsed result

    # print len(confs_list1)
    print "There are {0:d} conference records".format(len(confs_list))

    # print "*******"
    # for conf in confs_list2:
    #     print conf
    #     print "*******"

    schema = attribute2type.keys()
    confs_list = sorted(confs_list, key=lambda k: k['CONF_START'])  # sort confs by date, for later grouping by week
    csvfile = gzip.open(args.outdir + '/cms_conf_parsed.csv.gz', 'w')
    writer = csv.DictWriter(csvfile, fieldnames=schema,  delimiter='\t')
    csvfile.write(','.join(schema) + '\n')
    writer.writerows(confs_list)
    csvfile.close()


    # 2. group confs by week
    # grouped = group_confs_by_week(confs_list)
    grouped = group_confs_by_myweek(confs_list)
    
    # # output the grouped result
    # print "****group confs*****"
    # # import pdb; pdb.set_trace()
    # for yearweek,confs in grouped.iteritems():
    #     print "For week " + str(yearweek) + ":"
    #     print confs
    #     print "**********"

    # 3. count confs by week
    # confct_by_wk = count_confs_by_week(grouped)
    confct_by_wk = count_confs_by_myweek(grouped)
    
    # # output the conf ct per week
    # print "******count confs by week********"
    # for week, ct in confct_by_wk:
    #     print week, ct

    csvfile = gzip.open(args.outdir + '/cms_conf_ct_perweek.csv.gz', 'w')
    csvfile.write('tstamp,confct\n')
    for i in range(0,len(confct_by_wk)):
        # for week, ct in confct_by_wk:
    #     csvfile.write('{}-{},{}\n'.format(week, ct))
        if confct_by_wk[i][0][1] == 52:
            csvfile.write('{0}-{1},{2}\n'.format(mine_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 1).strftime('%Y%m%d'), datetime.date(confct_by_wk[i][0][0], 12, 31).strftime('%Y%m%d'), confct_by_wk[i][1]))
        else:
            csvfile.write('{0}-{1},{2}\n'.format(mine_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 1).strftime('%Y%m%d'), mine_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 7).strftime('%Y%m%d'), confct_by_wk[i][1]))
    csvfile.close()

    # 4. count confs in certain nb of future weeks, from each week
    # periods=[1,2,4,6,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85] # for next few weeks
    periods=[1,2,4,6,10,15,20,25,30,35,40,45,50,55,60,65,70] # for next few weeks
    confct_future = count_confs_in_future(confct_by_wk, periods)
        
    # print "***************"
    # for item in confct_future:
    #     print item

    csvfile = gzip.open(args.outdir + '/cms_conf_ct_future.csv.gz', 'w')
    header = 'tstamp,0wk'
    for i in range(0, len(periods)):
        header = header + ',' + str(periods[i]) + 'wk'
    csvfile.write(header + '\n')
    for i in range(0,len(confct_future)):
        # csvfile.write('{},{},{},{},{},{},{}\n'.format(iso_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 1).strftime('%Y%m%d'), iso_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 7).strftime('%Y%m%d'), confct_by_wk[i][1], confct_future[i][1], confct_future[i][2], confct_future[i][3], confct_future[i][4])) 
        if confct_by_wk[i][0][1] == 52:
            csvfile.write('{0}-{1}'.format(
                    mine_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 1).strftime('%Y%m%d'), datetime.date(confct_by_wk[i][0][0], 12, 31).strftime('%Y%m%d')))
            csvfile.write(',' + str(confct_by_wk[i][1]))
            for j in range(1,len(confct_future[i])):
                csvfile.write(',' + str(confct_future[i][j]))
            csvfile.write('\n')
        else:
            csvfile.write('{0}-{1}'.format(
                    mine_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 1).strftime('%Y%m%d'), mine_to_gregorian(confct_by_wk[i][0][0], confct_by_wk[i][0][1], 7).strftime('%Y%m%d')))
            csvfile.write(',' + str(confct_by_wk[i][1]))
            for j in range(1,len(confct_future[i])):
                csvfile.write(',' + str(confct_future[i][j]))
            csvfile.write('\n')
    csvfile.close()
