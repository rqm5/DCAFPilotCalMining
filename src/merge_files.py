#!/usr/bin/env python

import os
import re
import csv
import gzip
import argparse
import glob
import collections

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Merge data from dataset access and conf ct')
    parser.add_argument('--fda', dest='fda', help='input dir containing dataset access csv file')
    parser.add_argument('--fcc', dest='fcc', help='input conf ct csv.gzip file')
    parser.add_argument('--fmg', dest='fmg', help='output dir containing merged csv file')
    args = parser.parse_args()

    with gzip.open(args.fcc) as csvfile:
        reader = csv.DictReader(csvfile)
        fcc_lst= list(reader) # a list of dicts, each for a row in the csv file
        fcc_dct = collections.OrderedDict({indic['tstamp']:{k:indic[k] for k in indic if k != 'tstamp'} for indic in fcc_lst}) # convert the list of dicts to a dict with tstamp being the key
        
    dsfilenames = glob.glob(args.fda + '/dataframe*')
    for filename in  dsfilenames:

        print filename
        
        # read the dataset access file:
        with gzip.open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            fda_lst= list(reader) # a list of dicts, each for a row in the csv file
            fda_lst = [collections.OrderedDict(indic) for indic in fda_lst]

        # locate the row in conf ct file for the timestamp of the dataset access file, and merge them
        tstamp = re.search('\d{8}-\d{8}', filename).group()
        for dct in fda_lst:
            dct.update(fcc_dct[tstamp])
            
        # write merged to a file
        with gzip.open(args.fmg + '/' +  os.path.basename(filename), 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fda_lst[0].keys())
            writer.writeheader()
            writer.writerows(fda_lst)

            
        
