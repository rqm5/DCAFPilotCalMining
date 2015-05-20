#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author     : Ting Li <liting0612 At gmail dot com>
Description: Select records from dataset access files whose attribute is some value.
"""

import os
import re
import csv
import gzip
import argparse
import glob
# import ordereddict

''' Example:
select.py --indir original --attr tier --attrval 2 --outdir tier2
'''

def write_dct_lst(dct_lst, attrs, filename ):
    ''' write a list of dictionaries with common attributes to a file, according to specified order of attributes 
    input:
    dct_lst: a list of dictionaries
    attrs: a list of attributes' names, in a specified order
    output:
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

    parser = argparse.ArgumentParser(description='''Select records from dataset access files whose attribute is some value. 
    
Example:
select.py --indir original --attr tier --attrval 2 --outdir tier2''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--indir', dest='indir', help='a dir containing the csv.gz files for the input original dataset access data')
    parser.add_argument('--attr', dest='attr', help='a attribute to select by')
    parser.add_argument('--attrval', dest='attrval', help='a value of the attribute to select by')
    parser.add_argument('--outdir', dest='outdir', help='a dir cotaining csv.gz files for the output selected data')
    args = parser.parse_args()
    
    dsfilenames = glob.glob(args.indir + '/dataframe*')
    for filename in  dsfilenames:

        print filename

        # read in the header of dataset access file
        csvfile = gzip.open(filename)
        attrs = csvfile.readline().rstrip('\n').split(',') # a list of attribute names
        csvfile.close()
        
        # read the dataset access file:
        csvfile = gzip.open(filename)
        reader = csv.DictReader(csvfile)
        indir_lst= list(reader) # a list of dicts, each for a row in the csv file
        # indir_lst = [ordereddict.OrderedDict(dic) for dic in indir_lst]
        csvfile.close()

        # select examples/dcts whose attribute has a given value.
        select_dct_lst = [dct for dct in indir_lst if dct[args.attr] == args.attrval ]
        print len(select_dct_lst), len(indir_lst)

        # output to a file

        # directory = args.outdir + '/' + args.attr + '/' +  args.attrval
        directory = args.outdir
        if not os.path.exists(directory):
            print "create " + directory
            os.makedirs(directory)

        print directory  + '/'  +  os.path.basename(filename)
        write_dct_lst(select_dct_lst, attrs, directory  + '/'  +  os.path.basename(filename))

        # csvfile = gzip.open(directory  + '/'  +  os.path.basename(filename), 'wb')
        # writer = csv.DictWriter(csvfile, fieldnames=indir_lst[0].keys(), lineterminator='\n')
        # writer.writerow(dict(zip(writer.fieldnames, writer.fieldnames))) # works in python 2.6
        # # writer.writeheader() # works only in python 2.7
        # if len(select_dct_lst) > 0:
        #     writer.writerows(select_dct_lst)
        # csvfile.close()
            
        
if __name__ == '__main__':

    main()
