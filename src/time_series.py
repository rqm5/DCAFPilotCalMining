#!/usr/bin/env python

import os
import re
import csv
import gzip
import argparse
import glob
import datetime
import math
import numpy
from scipy.stats.stats import pearsonr
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

''' usage example:
datasetdir=/afs/cern.ch/user/t/tili/mywork/DCAFPilotCalMining/data
time_series.py --indir $datasetdir/tier/2/original      --inconf $datasetdir/conf/cms_conf_ct_perweek.csv.gz   --outdir $datasetdir/tier/2/datasets
'''

def crosscorr(lst1, lst2, index_match, lags):
    ''' Computer cross correlation between two time series lst1 and lst2, starting relative position of lst1 in lst2, over lag range lags
    input:
    lst1 and lst2: two lists for two time series
    index_match: an integer, for the relative time position of lst1 wrt lst2
    lags: a list for the range of lags
    output:
    cc: a dictionary with key being lag, and value being a pair of cross correlation and p-value for testing uncorrelatedness null
    '''

    ts1 = lst1
    
    cc = {}
    for lag in lags:

        ts2 = [0]*len(ts1)
        index_start = max(index_match + lag, 0)
        len_prefix0 = index_start - (index_match + lag)
        index_end = min(index_match + lag + len(ts1), len(lst2))
        ts2[len_prefix0 : len_prefix0 + (index_end - index_start)] = lst2[index_start:index_end]

        # cc[lag] = numpy.corrcoef(ts1, ts2)[0, 1]
        try:
            cc[lag] = pearsonr(ts1, ts2)
        except:
            pass

        # import math
        # if math.isnan(cc[lag][0]):
        #     print lag
        #     raw_input('enter <return> to continue .. ')

    return cc


def fft_half_spectrum(signal):
    ''' Computer DFT of a time series signal by FFT
    Input:
    signal: a list for a time series
    output:
    [mgft,freqs]: a list of sublists for DFT magnitudes and frquencies, over the positive half frequency range [0,0.5]
    '''

    signal = signal - numpy.mean(signal)
    ft = numpy.fft.rfft(signal*numpy.hanning(len(signal))) # rfft() return the positive half [0,0.5]  of the freq spectrum [-0.5, 0.5]
    mgft = abs(ft)

    xVals = numpy.fft.fftfreq(len(signal), d=1.0) # in week
    freqs = xVals[:len(mgft)]
    freqs[-1] = abs(freqs[-1]) # if the signal is even in length, the positive half of the freq spectrum should be half of the langth plus 1, with the last freq being 0.5, instead of -0.5

    return [mgft, freqs]

def group_by_dataset_and_extract_access (dct_lst):
    ''' Convert a list of dicts (for records), into a list of dicts (each for a dataset) which are sorted by dataset length, and the records of each datset is sorted by timestamp.
    input:
    dct_lst: a list of dicts, each dict for a record
    output:
    lst_dataset_week_naccess: a list of dicts, each dict for a dataset and with keys 'dataset_dbs', 'length', 'tstamp', and 'naccess'
    '''

    # (a) group the records by [dataset, dbs], 
    # convert a list of records (as dicts), to a dict: key being (dataset, dbs), and value being a list of its records (as dicts). 
    dct_dataset = {}
    for dct in dct_lst:
        if (dct['dataset'],dct['dbs']) in dct_dataset:
            dct_dataset[(dct['dataset'],dct['dbs'])].append(dct)
        else:
            dct_dataset[(dct['dataset'],dct['dbs'])] = [dct]

    # (b) sort the datasets by their lengths, and sort the records of each dataset by timestamp

    # convert from dict to a list of lists (each for a dataset) of records (as dicts), and sort datasets by length of records i.e. dcts
    lst_dataset = dct_dataset.values()
    def mycmp1(lst1, lst2):
        if len(lst1) > len(lst2):
            return -1
        elif len(lst1) < len(lst2):
            return 1
        else:
            return 0
    lst_dataset_sorted = sorted(lst_dataset, cmp=mycmp1)

    # for each dataset, sort its records i.e. dcts, by their time stamps
    def mycmp2(dct1, dct2):
        if int(dct1['tstamp'][0:7]) > int(dct2['tstamp'][0:7]):
            return 1
        if int(dct1['tstamp'][0:7]) < int(dct2['tstamp'][0:7]):
            return -1
        else:
            return 0
    tmp = [sorted(lst,cmp=mycmp2) for lst in lst_dataset_sorted]
    lst_dataset_sorted = tmp


    # (c) for each dataset (dataset,dbs), extract from each record only naccess and tstamp and ignore other attributes, and add missing weeks with naccess value 0
    lst_dataset_week_naccess = []
    for lst in lst_dataset_sorted:
        dct={}
        dct['dataset_dbs'] = (lst[0]['dataset'], lst[0]['dbs'])
        dct['tstamp'] = []
        dct['naccess'] = []
        dct['length'] = len(lst)
        for i in range(0, len(lst)):
            dct['tstamp'].append(lst[i]['tstamp'])
            dct['naccess'].append(float(lst[i]['naccess']))
            # add missing weeks with naccess value 0
            if i != len(lst)-1:
                date1 = datetime.date(int(lst[i]['tstamp'][-8:-4]), int(lst[i]['tstamp'][-4:-2]), int(lst[i]['tstamp'][-2:]))
                date2 = datetime.date(int(lst[i+1]['tstamp'][0:4]), int(lst[i+1]['tstamp'][4:6]), int(lst[i+1]['tstamp'][6:8]))
                while (date2 - date1).days > 1:
                    dct['tstamp'].append('{0}-{1}'.format((date1+datetime.timedelta(days=1)).strftime('%Y%m%d'), (date1+datetime.timedelta(days=7)).strftime('%Y%m%d')))
                    dct['naccess'].append(0)
                    date1 = date1 + datetime.timedelta(days=7)
        lst_dataset_week_naccess.append(dct)

    return lst_dataset_week_naccess



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generates time series for each dataset from the dataset access data, and analyze the cross correlations and seasonalities for the time series of dataset access and time series of conferenct count')
    parser.add_argument('--indir', dest='indir', help='a dir containing csv.gz files for the input dataset access data')
    parser.add_argument('--inconf', dest='inconf', help='a csv.gz file for the input conference count data')
    parser.add_argument('--outdir', dest='outdir', help='a dir containing csv.gz files for the output time series of each dataset, and image files for the plots of cross correlation and FFT of the time series')
    args = parser.parse_args()

######################
######## 1. read in dataset access records, and group them by dataset and extract only access and timestamp info.

    # (1) read all dataframe files into a list of dicts, each dict for a record (i.e. a row in a dataframe file)
    dct_lst = []
    header = False
    dsfilenames = glob.glob(args.indir + '/dataframe*')
    for filename in  dsfilenames:

        # print filename

        # read the header of dataset access file into a list
        if header == False:
            csvfile = gzip.open(filename)
            attrs = csvfile.readline().rstrip('\n').split(',') # a list of attribute names
            csvfile.close()        
            attrs.append('tstamp')
            header = True
        
        # read the dataset access file:
        csvfile = gzip.open(filename)
        reader = csv.DictReader(csvfile)
        indir_lst= list(reader) # a list of dicts, each for a row in the csv file
        csvfile.close()

        # add a tstamp attribute to each dict in the list
        tstamp = re.search('\d{8}-\d{8}', filename).group()
        for dct in indir_lst:
            dct.update({'tstamp': tstamp})

        dct_lst = dct_lst + indir_lst    


    # (2) group them by dataset and extract only access and timestamp info.
    lst_dataset_week_naccess = group_by_dataset_and_extract_access (dct_lst)


    # (3) write time series of each dataset to a file
    csvfile = gzip.open(args.outdir + '/time_series_per_dataset.csv.gz', 'w')
    # write header
    csvfile.write('Number of (dataset,dbs)\'s: ' + str(len(lst_dataset_week_naccess)) + '\n\n')    
    # write data
    for dct in lst_dataset_week_naccess:
        csvfile.write('Length: '  + str(len(dct['tstamp'])) + ' dataset: ' + dct['dataset_dbs'][0] + ' dbs: ' + dct['dataset_dbs'][1] + '\n')
        csvfile.write('tstamp,naccess\n')
        for i in range(0, len(dct['tstamp'])):
            csvfile.write(dct['tstamp'][i] + ',' + str(dct['naccess'][i]) + '\n')
        csvfile.write('\n')
    csvfile.close()
            

    # (4) The number of records, access count mean, and access count standard deviation of each dataset, and their statistics over all the datasets

    print '********************'
    print 'Statistics of datasets'

    # collect the information about each dataset
    lengths = []
    stds = []
    means = []
    for i in range(0, len(lst_dataset_week_naccess)):

        dct = lst_dataset_week_naccess[i]

        lengths.append(dct['length'])
        stds.append(numpy.std(dct['naccess']))
        means.append(numpy.mean(dct['naccess']))

    print 'nb of datasets:' + str(len(lengths))
    print 'nb of datasets whose access series lengths are longer than 10 and are not constant:' + str(len([i for i in range(0, len(lengths)) if lengths[i] > 10 and stds[i] > 0]))

    
    # for length of each dataset
    # histogram in text
    print 'Histogram of lengths of datasets:'
    numBins = 50
    print numpy.histogram(lengths, numBins)
    # histogram in plot
    fig = plt.figure()
    plt.hist(lengths, numBins,color='green',alpha=0.8)
    plt.xlabel('Length')
    plt.title('Histogram of Lengths of Datasets')
    pp = PdfPages(args.outdir + '/' 'dataset_' + str(len(lengths)) + '_lengths_hist.pdf')
    pp.savefig(fig)
    pp.close()
    # statistics
    print 'max, min, median, and std of lengths of datasets:'
    print max(lengths), min(lengths), numpy.median(lengths), numpy.std(lengths)
    print 'nb of datasets whose access series are longer than 10:'
    print len([x for x in lengths if x > 10])
    
    # for access count std of each dataset
    # histogram in text
    print 'Histogram of access count stds of datasets:'
    numBins = 100
    print numpy.histogram(stds, numBins)
    # histogram in plot
    fig = plt.figure()
    plt.hist(stds, numBins,color='green',alpha=0.8)
    plt.xlabel('Access Count STD')
    plt.title('Histogram of Access Count STDs of Datasets')
    pp = PdfPages(args.outdir + '/' 'dataset_' + str(len(stds)) + '_stds_hist.pdf')
    pp.savefig(fig)
    pp.close()
    # statistics
    print 'max, min, median, and std of access count stds of datasets:'
    print max(stds), min(stds), numpy.median(stds), numpy.std(stds)
    print 'nb of datasets whose access series are constant:'
    print len([x for x in stds if x == 0])

    # for access count mean of each dataset
    # histogram in text
    print 'Histogram of access count means of datasets:'
    numBins = 100
    print numpy.histogram(means, numBins)
    # histogram in plot
    fig = plt.figure()
    plt.hist(means, numBins,color='green',alpha=0.8)
    plt.xlabel('Access Count Mean')
    plt.title('Histogram of Access Count Means of Datasets')
    pp = PdfPages(args.outdir + '/' 'dataset_' + str(len(means)) + '_means_hist.pdf')
    pp.savefig(fig)
    pp.close()
    # statistics
    print 'max, min, median, and std of access count means of datasets:'
    print max(means), min(means), numpy.median(means), numpy.std(means)
    print 'nb of datasets whose access series are constantly zero:'
    print len([x for x in means if x == 0])

    # import sys; sys.exit(0)

################
########### 2. read in the conference count series

    csvfile = gzip.open(args.inconf)
    reader = csv.DictReader(csvfile)
    inconf_lst= list(reader)  # a list of dicts, each for a row in the csv file
    # convert the list of dicts to a dict with tstamp and confct being the key
    inconf_dct = {'tstamp':[], 'confct':[]}
    inconf_dct['tstamp'] = [dct['tstamp'] for dct in inconf_lst]
    inconf_dct['confct'] = [float(dct['confct']) for dct in inconf_lst]
    csvfile.close()


######################
########## 3. crosscorrelation between a dataset access series and the conference count series

    print '********************'
    print 'Cross correlation'
    
    lags = range(-90,90)
    max_crosscorr = [] # (lag, (crosscorr, p-value))
    for i in range(0, len(lst_dataset_week_naccess)): # lst_dataset_week_naccess is a list of dicts, each for a dataset
        dct = lst_dataset_week_naccess[i]

        if dct['length'] < 10 or numpy.var(dct['naccess']) == 0: # only consider datasets with more than 10 records, to correlate with conference count series, and which has nonzero variance in naccess
            continue

        # (1) match the dataset access series to the conference count series, by timestamp of the start of the dataset access series
        index_match = inconf_dct['tstamp'].index(dct['tstamp'][0])
        # cross correlation over a range of lags wrt the match timestamp
        dct['crosscorr'] = crosscorr(dct['naccess'], inconf_dct['confct'], index_match, lags)


        # (2) plot cross correlation versus lags, and save it (already done, run just once)
        fig = plt.figure()

        ax1 = fig.add_subplot(311)
        cc = [dct['crosscorr'][lag][0] for lag in lags]
        ax1.bar(lags, cc, width=0.1, edgecolor='None',color='k',align='center')
        ax1.grid(True)
        ax1.axhline(0, color='black', lw=2)
        ax1.set_xlabel('Lag')
        ax1.set_ylabel('Cross Correlation')
        ax1.set_title('dataset: ' + dct['dataset_dbs'][0] + ' dbs: ' + dct['dataset_dbs'][1])

        # plot the two time series as well

        ax2 = fig.add_subplot(312)
        ax2.plot(range(0, len(inconf_dct['confct'])), [0] * len(inconf_dct['confct']), 'k', range(index_match, index_match + len(dct['naccess'])), dct['naccess'], 'b', lw=1)
        ax2.grid(True)
        ax2.axhline(0, color='black', lw=2)
        ax2.set_xlabel('Week')
        ax2.set_ylabel('Dataset naccess')
        
        ax3 = fig.add_subplot(313)
        ax3.plot(range(0, len(inconf_dct['confct'])), inconf_dct['confct'], 'r', lw=1)
        ax3.grid(True)
        ax3.axhline(0, color='black', lw=2)
        ax3.set_xlabel('Week')
        ax3.set_ylabel('Conference Count')

        # plt.xlabel('Week')

        pp = PdfPages(args.outdir + '/' + '_'.join(dct['dataset_dbs']) + '_' + str(index_match) + '.pdf')
        pp.savefig(fig)
        pp.close()


        # (3) find the lag with the hightest cross correlation
        lst = dct['crosscorr'].items()
        def mycmp3(lst1, lst2):
            # if abs(lst1[1][0]) > abs( lst2[1][0]): 
            if lst1[1][0] > lst2[1][0]:  # consider signed correlation, instead of its magnitude.
                return 1
            # elif abs(lst1[1][0]) < abs(lst2[1][0]):
            elif lst1[1][0] < lst2[1][0]:
                return -1
            else:
                return 0
        tmp = sorted(lst, cmp = mycmp3)
        max_crosscorr.append(tmp[-1]) 



    print '********************'
    print 'Lags for max cross correlation'

    # print max_crosscorr
    max_crosscorr_lags = [x[0] for x in max_crosscorr if x[1][1] < 0.05] # check if p value for each cross correlation is small, i.e. rejecting uncorrelatedness
    numBins = 20
    print numpy.histogram(max_crosscorr_lags, numBins)
    # plot
    fig = plt.figure()
    plt.hist(max_crosscorr_lags, numBins,color='green',alpha=0.8)
    plt.xlabel('Lag')
    plt.title('Histogram for Lags for Max Cross Correlations')
    pp = PdfPages(args.outdir + '/' 'lag_' + str(len(max_crosscorr_lags)) + '_hist.pdf')
    pp.savefig(fig)
    pp.close()

###################
#########  4. compute FFT of conference count series, and FFT of each dataset access series. Check their periodocities from their FFTs
    
    print '********************'
    print 'FFT'

    # (1) find the period of conf ct series, by fft
    signal = inconf_dct['confct']
    [mgft, freqs] = fft_half_spectrum(signal)

    fig = plt.figure()

    ax1 = fig.add_subplot(211)
    ax1.bar(freqs, mgft, width=0.001, edgecolor='None',color='k',align='center')
    ax1.set_ylabel('DFT')
    ax1.set_xlabel('Frequency')
    ax1.set_title('Conference')

    # plot the two time series as well
    ax2 = fig.add_subplot(212)
    ax2.plot(range(0, len(signal)), signal, 'b', lw=1)
    ax2.grid(True)
    ax2.axhline(0, color='black', lw=2)
    ax2.set_xlabel('Week')
    ax2.set_ylabel('Conference Count')

    pp = PdfPages(args.outdir + '/' + 'conf_ct_perweek_' + str(len(signal)) + 'fft.pdf')
    pp.savefig(fig)
    pp.close()

    
    # (2) find the period of each dataset's naccess series, by fft 
    for i in range(0, len(lst_dataset_week_naccess)):
        dct = lst_dataset_week_naccess[i]
        if dct['length'] < 10 or numpy.var(dct['naccess']) == 0: # only consider datasets with more than 10 records, to correlate with conference count series, and which has non zero variance in naccess
            continue

        signal = dct['naccess']
        [mgft, freqs] = fft_half_spectrum(signal)

        fig = plt.figure()

        ax1 = fig.add_subplot(211)
        ax1.bar(freqs, mgft, width=0.001, edgecolor='None',color='k',align='center')
        ax1.set_ylabel('DFT')
        ax1.set_xlabel('Frequency')
        ax1.set_title('dataset: ' + dct['dataset_dbs'][0] + ' dbs: ' + dct['dataset_dbs'][1])

        # plot the two time series as well
        ax2 = fig.add_subplot(212)
        ax2.plot(range(0, len(dct['naccess'])), dct['naccess'], 'b', lw=1)
        ax2.set_xlabel('Week')
        ax2.set_ylabel('Dataset naccess')

        pp = PdfPages(args.outdir + '/' + '_'.join(dct['dataset_dbs']) + '_' + str(len(signal)) + '_fft.pdf')
        pp.savefig(fig)
        pp.close()
