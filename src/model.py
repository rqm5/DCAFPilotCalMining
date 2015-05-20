#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : model.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>, modified by Ting <liting0612 AT gmail dot com>
Description: Generic classification model template
"""

# system modules
import os
import sys
import time
import random
#import pprint
#try:
#    import cPickle as pickle
#except:
#    import pickle

# NumPy and pandas
import numpy as np
import pandas as pd

# sklearn modules
from sklearn.cross_validation import train_test_split
from sklearn import preprocessing
from sklearn.metrics.scorer import SCORERS
from sklearn.pipeline import make_pipeline
from sklearn import metrics

# local modules
from DCAF.ml.utils import OptionParser, normalize, logloss, GLF
from DCAF.ml.clf import learners, param_search, crossvalidation, print_clf_report
import DCAF.utils.jsonwrapper as json

def files(idir, ext=".csv.gz"):
    "Return list of files from given directory"
    for fname in os.listdir(idir):
        if  fname.endswith(ext):
            yield '%s/%s' % (idir, fname)

def get_auc(labels, predictions):
    fpr, tpr, thresholds = metrics.roc_curve(labels, predictions, pos_label=1)
    auc = metrics.auc(fpr,tpr)
    return auc

def read_data(fname, drops=[], idx=0, limit=-1, scaler=None):
    "Read and return processed data frame"
    comp = None
    if  fname.endswith('.gz'):
        comp = 'gzip'
    elif  fname.endswith('.bz2'):
        comp = 'bz2'
    if  scaler:
        print fname, comp, np.float32
        xdf = pd.read_csv(fname, compression=comp, dtype=np.float32)
    else:
        xdf = pd.read_csv(fname, compression=comp)
    # fill NAs
    xdf = xdf.fillna(0)
    # drop fields
    if  drops:
        xdf = xdf.drop(drops, axis=1)
    # drop duplicates
#    xdf = xdf.drop_duplicates(take_last=True, inplace=False)
    if  limit > -1:
        xdf = xdf[idx:limit]
    return xdf

def factorize(col, xdf, sdf=None):
    "Factorize given column in dataframe"
    if  sdf:
        vals = set(xdf[col] + sdf[col])
    else:
        vals = set(xdf[col])
    ids = []
    for uid, val in enumerate(vals):
        ids.append(uid)
    dval = dict(zip(vals, ids))
    out = []
    for val in xdf[col]:
        out.append(dval[val])
    return out

def model(train_file, newdata_file, idcol, tcol, learner, lparams=None,
        drops=None, split=0.3, scorer=None,
        scaler=None, ofile=None, idx=0, limit=-1, gsearch=None, crossval=None, verbose=False):
    """
    Build and run ML algorihtm for given train/test dataframe
    and classifier name. The learners are defined externally
    in DCAF.ml.clf module.
    """
    split = 0 # change by Ting to use the whole training set for training, not for validation. 

    clf = learners()[learner]
    if  lparams:
        if  isinstance(lparams, str):
            lparams = json.loads(lparams)
        elif isinstance(lparams, dict):
            pass
        else:
            raise Exception('Invalid data type for lparams="%s", type: %s' % (lparams, type(lparams)))
        for key, val in lparams.items():
            setattr(clf, key, val)
    setattr(clf, "random_state", 123) 

    print clf
    if  split:
        if  isinstance(split, int):
            split = split/100.
        elif isinstance(split, float):
            pass
        elif isinstance(split, basestring):
            split = float(split)
        print "Split level: train %s%%, validation %s%%" % (round((1-split)*100), round(split*100))
    if  verbose:
        print "idx/limit", idx, limit

    # read data and normalize it
    if  drops:
        if  isinstance(drops, basestring):
            drops = drops.split(',')
        if  idcol not in drops:
            drops += [idcol]
    else:
        drops = [idcol]
    xdf = read_data(train_file, drops, idx, limit, scaler)

    # get target variable and exclude choice from train data
    target = xdf[tcol]
    xdf = xdf.drop(tcol, axis=1)
    if  verbose:
        print "Train file", train_file
        print "Columns:", ','.join(xdf.columns)
        print "train shapes:", xdf.shape, target.shape
        if  verbose>1:
            print "Target:", tcol, target

    # split our train data
    if  split:
        # x_train, x_rest, y_train, y_rest = train_test_split(xdf, target, test_size=split) 
        x_train, x_rest, y_train, y_rest = train_test_split(xdf, target, test_size=split, random_state=1234) # change by Ting, for controlling random seed
        if  verbose:
            print "train shapes after splitting:", x_train.shape, y_train.shape
    else:
        x_train = xdf
        y_train = target
        x_rest = None
        y_rest = None
    if  gsearch:
        param_search(clf, x_train, y_train, x_rest, y_rest, gsearch)
        sys.exit(0)
    if  crossval:
        crossvalidation(clf, xdf, target)
        sys.exit(0)

    ###############################################################################
    # add by Ting to do feature selection and measuare feature importance
    # Univariate feature selection with F-test for feature scoring
    # We use the default selection function: the 10% most significant features
    from sklearn.feature_selection import SelectKBest, chi2, SelectPercentile, f_classif

    selector = SelectPercentile(f_classif, percentile=100) # by F test
    selector.fit(x_train, y_train)
    pvs = selector.pvalues_
    # output scores of features
    columns = xdf.columns
    indices = np.argsort(pvs)
    num = len(columns) 
    print("\n Feature ranking by ANOVA F test:")
    for f in range(num):
        print("%d. feature selection test p-value %f, feature %s" % (f + 1, pvs[indices[f]], columns[indices[f]]))


    selector = SelectPercentile(chi2, percentile=10) # by chi square test
    selector.fit(x_train, y_train)
    pvs = selector.pvalues_
    # output scores of features
    columns = xdf.columns
    indices = np.argsort(pvs)
    num = len(columns) 
    print("\n Feature ranking by Chi Squared test:")
    for f in range(num):
        print("%d. feature selection test p-value %f, feature %s" % (f + 1, pvs[indices[f]], columns[indices[f]]))

    
    ###############################################################################

    # preprocessing of "scaler" type
    # scaler = None  # added by ting, to ignore the standardization, but fail to do that. todo
    if  scaler:
        x_train = getattr(preprocessing, scaler)().fit_transform(x_train)

    time0 = time.time()
    fit = clf.fit(x_train, y_train)
    if  verbose:
        print "Train elapsed time", time.time()-time0
        
    # comment out by Ting, move it to the new test dataset
    # # for validation
    # if  split:
    #     predictions = fit.predict(x_rest)
    #     try:
    #         importances = clf.feature_importances_
    #         if  importances.any():
    #             print "Feature ranking:"
    #             columns = xdf.columns
    #             indices = np.argsort(importances)[::-1]
    #             # num = 9 if len(columns)>9 else len(columns) 
    #             num = len(columns) # change by Ting
    #             for f in range(num):
    #                 print("%d. importance %f, feature %s" % (f + 1, importances[indices[f]], columns[indices[f]]))
    #     except:
    #         pass
    #     if  scorer:
    #         for scr in scorer.split(','):
    #             scr_str = repr(metrics.SCORERS[scr]).replace('make_scorer(', '').replace(')', '')
    #             method = scr_str.split(',')[0]
    #             res = getattr(metrics, method)(y_rest, predictions)
    #             print "Score metric (%s): %s" % (method, res)
    #     if  verbose:
    #         loss = 0
    #         tot = 0
    #         for pval, yval in zip(predictions, y_rest):
    #             if  verbose>1:
    #                 print "predict value %s, real value %s" % (pval, yval)
    #             loss += logloss(pval, yval)
    #             tot += 1
    #         print "Final Logloss", loss/tot
    # else:
    #     print "Since there is no train/validation splitting, no prediction metrics will be shown"

    # predict on new data set, by the learned classifier
    if  newdata_file:
        tdf = read_data(newdata_file, drops, scaler=scaler)
        if  tcol in tdf.columns:
            tdf = tdf.drop(tcol, axis=1)
        if  verbose:
            print "New data file", newdata_file
            print "Columns:", ','.join(tdf.columns)
            print "test shapes:", tdf.shape
        datasets = [int(i) for i in list(tdf['dataset'])]
        dbses = [int(i) for i in list(tdf['dbs'])]
        if  scaler:
            tdf = getattr(preprocessing, scaler)().fit_transform(tdf)
        predictions = fit.predict(tdf)
        data = {'dataset':datasets, 'dbs': dbses, 'prediction':predictions}
        out = pd.DataFrame(data=data)
        if  ofile:
            out.to_csv(ofile, header=True, index=False)

        try:
            importances = clf.feature_importances_
            if  importances.any():
                print "\n Feature ranking by random forest classifier:"
                columns = xdf.columns
                indices = np.argsort(importances)[::-1]
                # num = 9 if len(columns)>9 else len(columns)  
                num = len(columns) # change by Ting, to output all features' importances
                for f in range(num):
                    print("%d. importance %f, feature %s" % (f + 1, importances[indices[f]], columns[indices[f]]))
        except:
            pass

def model_iter(train_file_list, newdata_file, idcol, tcol,
    learner, lparams=None, drops=None, split=0.1, scaler=None, ofile=None, verbose=False):
    """
    Build and run ML algorihtm for given train/test dataframe
    and classifier name. The learners are defined externally
    in DCAF.ml.clf module.
    """
    if  learner not in ['SGDClassifier', 'SGDRegressor']:
        raise Exception("Unsupported learner %s" % learner)
    clf = learners()[learner]
    if  lparams:
        if  isinstance(lparams, str):
            lparams = json.loads(lparams)
        elif isinstance(lparams, dict):
            pass
        else:
            raise Exception('Invalid data type for lparams="%s", type: %s' % (lparams, type(lparams)))
        for key, val in lparams.items():
            setattr(clf, key, val)
    print "clf:", clf

    if  drops:
        if  isinstance(drops, basestring):
            drops = drops.split(',')
        if  idcol not in drops:
            drops += [idcol]
    else:
        drops = [idcol]
    fit = None
    for train_file in train_file_list:
        print "Train file", train_file
        # read data and normalize it
        xdf = read_data(train_file, drops, scaler=scaler)

        # get target variable and exclude choice from train data
        target = xdf[tcol]
        xdf = xdf.drop(tcol, axis=1)
        if  verbose:
            print "Columns:", ','.join(xdf.columns)
            print "Target:", target

        if  scaler:
            xdf = getattr(preprocessing, scaler)().fit_transform(xdf)
        if  split:
            x_train, x_rest, y_train, y_rest = \
                    train_test_split(xdf, target, test_size=0.1)
            time0 = time.time()
            fit = clf.partial_fit(x_train, y_train)
            if  verbose:
                print "Train elapsed time", time.time()-time0
            print "### SCORE", clf.score(x_rest, y_rest)
        else:
            x_train = xdf
            y_train = target
            time0 = time.time()
            fit = clf.partial_fit(x_train, y_train)
            if  verbose:
                print "Train elapsed time", time.time()-time0

    # new data for which we want to predict
    if  newdata_file:
        tdf = read_data(newdata_file, drops, scaler=scaler)
        if  tcol in tdf.columns:
            tdf = tdf.drop(tcol, axis=1)
        datasets = [int(i) for i in list(tdf['dataset'])]
        dbses = [int(i) for i in list(tdf['dbs'])]
        if  scaler:
            tdf = getattr(preprocessing, scaler)().fit_transform(tdf)
        predictions = fit.predict_proba(tdf)
        data = {'dataset':datasets, 'dbs': dbses, 'prediction':predictions}
        out = pd.DataFrame(data=data)
        if  ofile:
            out.to_csv(ofile, header=True, index=False)

def main():
    "Main function"
    optmgr = OptionParser(learners().keys(), SCORERS.keys())
    opts, _ = optmgr.options()
    if  opts.learner_help:
        obj = learners()[opts.learner_help]
        print obj
        print obj.__doc__
        sys.exit(0)
    ofile = opts.predict
    if  not ofile:
        ofile = "%s.predictions" % opts.learner
    model2run = 'model'
    if  opts.train.find(',') != -1: # list of files
        train_files = opts.train.split(',')
        model2run = 'model_iter'
    elif os.path.isdir(opts.train): # we got directory name
        for ext in ['.csv.gz', '.csv']:
            train_files = [f for f in files(opts.train, ext)]
            model2run = 'model_iter'
            if  len(train_files):
                break

    random.seed(12345) 
    if  model2run == 'model_iter':
        model_iter(train_file_list=train_files, newdata_file=opts.newdata,
                idcol=opts.idcol, tcol=opts.target,
                learner=opts.learner, lparams=opts.lparams,
                drops=opts.drops, split=opts.split,
                scaler=opts.scaler, ofile=ofile, verbose=opts.verbose)
    else:
        model(train_file=opts.train, newdata_file=opts.newdata,
                idcol=opts.idcol, tcol=opts.target,
                learner=opts.learner, lparams=opts.lparams,
                drops=opts.drops, split=opts.split,
                scorer=opts.scorer, scaler=opts.scaler, ofile=ofile,
                idx=opts.idx, limit=opts.limit, gsearch=opts.gsearch,
                crossval=opts.cv, verbose=opts.verbose)

if __name__ == '__main__':
    main()
