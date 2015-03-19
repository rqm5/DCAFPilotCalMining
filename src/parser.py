#!/usr/bin/env python

"""
This is a parser with:
input:
a csv/csv.gz data file dumped from ORACLE DB. (dump file contains extra spaces, newlines, etc.)
a schema file
output:
a list of dictionaries, each of which represents the parsed result of each data example against the schema

it is a stand-alone python script, and run by
python parser.py --fin=cms_conf.csv.gz --schema=schema 
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
"""

import argparse
import datetime
import re
import gzip
import collections



def type_db2py(dbtype):
    """ convert from db types to python types
    input: a string which represents a db type
    output: a python type"""

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
    """    
    convert a date string (input) to a datetime.date object (output)
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
    """
parse a schema file for the type of each field
input: a schema file
output: an ordered dictionary of each schema attribute and its type
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
    """
Parse a dataframe file, by specifying record separator and file separator and splitting according to the separtors
 input: a dataframe file, and an ordered dict of each schema attribute and its type
 output: a list of dicts, each of which is the parsed result of each conference by the schema
it assumes that each field can't span more than one lines.
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
    """  
parse dataframe, by specifying each record and reach field
 input: a dataframe file, and an ordered dict of each schema attribute and its type
 output: a list of dicts, each of which is the parsed result of each conference by the schema
allow PRES_TITLE field span more than one lines, and assume other fields can't span more than one line
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
        


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Parse some dataframe.')
    parser.add_argument('--fin', dest='fdataframe', help='input dataframe file')
    parser.add_argument('--schema', dest='fschema', help='input schema file')    
    args = parser.parse_args()

    
    attribute2type = parse_schema(args.fschema)

    # two ways of parsing dataframe: (prefer the second way over the first)
    # by specifying separtors for separating records and fields, assume each field of a record only spans one line
    # confs_list1 = parse_dataframe_by_split(args.fdataframe, attribute2type)
    # by matching each record and field.  allow PRES_TITLE field span more than one lines, and assume other fields can't span more than one line
    confs_list2 = parse_dataframe_by_match_record(args.fdataframe, attribute2type)    

    # examine the parsed result

    # print len(confs_list1)
    print "There are {:d} conference records".format(len(confs_list2))
    print "*******"
    
    for conf in confs_list2:
        print conf
        print "*******"
    
