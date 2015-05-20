#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author     : Ting Li <liting0612 At gmail dot com>
Description: Add records from conference counts to dataset access records.
"""


import os
import re
import csv
import gzip
import argparse
import glob

def write_dct_lst(dct_lst, attrs, filename ):
    ''' write a list of dictionaries with common attributes to a file, according to specified order of attributes 
    dct_lst: a list of dictionaries
    attrs: a list of attributes' names, in a specified order
    filename: output file
    '''

    csvfile = gzip.open(filename, 'w')
    # write header
    csvfile.write(','.join(attrs) + '\n')    
    # write data
    for dct in dct_lst:
        line = []
        for attr in attrs:
            line.append(dct[attr])
        csvfile.write(','.join(line) + '\n')
    csvfile.close()


def main():

    parser = argparse.ArgumentParser(description='''Add records from conference counts to dataset access records.

Example:
merge_access_conf.py --indir original    --inconf cms_conf_ct_future.csv.gz --outdir merged''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--indir', dest='indir', help='a dir containing the csv.gz files for the input dataset access data, assuming the data filenames start with "dataframe" ')
    parser.add_argument('--inconf', dest='inconf', help='a csv.gz file for the input conference count data')
    parser.add_argument('--outdir', dest='outdir', help='a dir containing csv.gz files for the output merged data')
    args = parser.parse_args()

    # read in the header of conference count data file
    csvfile = gzip.open(args.inconf)
    attrs_inconf = csvfile.readline().rstrip('\n').split(',')[1:] # a list of attribute names, ignore the first attr "tstamp"
    csvfile.close()

    # read in the conference count data
    csvfile = gzip.open(args.inconf)
    reader = csv.DictReader(csvfile)
    inconf_lst= list(reader) # [ordereddict.OrderedDict(zip(keys,row)) for row in reader ] # # a list of dicts, each for a row in the csv file
    ## inconf_dct = ordereddict.OrderedDict({indic['tstamp']:{k:indic[k] for k in indic if k != 'tstamp'} for indic in inconf_lst}) # convert the list of dicts to a dict with tstamp being the key
    # inconf_dct = ordereddict.OrderedDict((indic['tstamp'],ordereddict.OrderedDict((k,indic[k]) for k in indic if k != 'tstamp')) for indic in inconf_lst) # convert the list of dicts to a dict with tstamp being the key
    inconf_dct = dict((indic['tstamp'],dict((k,indic[k]) for k in indic if k != 'tstamp')) for indic in inconf_lst) # convert the list of dicts to a dict with tstamp being the key
    csvfile.close()
    

    dsfilenames = glob.glob(args.indir + '/dataframe*')
    for filename in  dsfilenames:

        print filename

        # read in the header of dataset access file
        csvfile = gzip.open(filename)
        attrs_indir = csvfile.readline().rstrip('\n').split(',') # a list of attribute names
        csvfile.close()
        
        # read the dataset access file:
        csvfile = gzip.open(filename)
        reader = csv.DictReader(csvfile)
        indir_lst= list(reader) # a list of dicts, each for a row in the csv file
        # indir_lst = [ordereddict.OrderedDict(indic) for indic in indir_lst]
        csvfile.close()

        # locate the row in conf ct file for the timestamp of the dataset access file, and merge them
        tstamp = re.search('\d{8}-\d{8}', filename).group()
        for dct in indir_lst:
            dct.update(inconf_dct[tstamp])
            
        # write merged data to a file
        attrs = attrs_indir + attrs_inconf
        write_dct_lst(indir_lst, attrs, args.outdir + '/' +  os.path.basename(filename))
            
        # # write merged to a file
        # csvfile = gzip.open(args.outdir + '/' +  os.path.basename(filename), 'w')
        # writer = csv.DictWriter(csvfile, fieldnames=indir_lst[0].keys(), lineterminator='\n')
        # writer.writerow(dict(zip(writer.fieldnames, writer.fieldnames))) # works in python 2.6
        # # writer.writeheader() # works only in python 2.7
        # writer.writerows(indir_lst)
        # csvfile.close()
            
        
if __name__ == '__main__':

    main()

