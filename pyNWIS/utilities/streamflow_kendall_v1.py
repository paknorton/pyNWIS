#!/usr/bin/env python
"""This script reads files of downloaded NWIS annual observations and
station information and computes the Kendall tau for each station
within a specified date range. Results and associated station information
are written to a tab-delimited output file.
"""

#      Author: Parker Norton
#        Date: 2014-09-09
# Description: Reads NWIS data that was pulled using XXXXXX.py
#              and computes the Kendall Tau for each column of data for a subset
#              based on a date range. The Kendal Tau results are then written to
#              a tab-delimited file.
#       Notes:
#              2015-10-14 PAN: Updated to version 0.2

from __future__ import (absolute_import, division, print_function)
from future.utils import iteritems, itervalues

import os
import platform
import sys
import pandas as pd
import numpy as np
import datetime
import kendall_cy as nr3
from dateutil.relativedelta import relativedelta
from time import strftime
import argparse
import calendar
from collections import Counter

__author__ = 'Parker Norton (pnorton@usgs.gov)'
__version__ = '0.2'

# 2015-10-14 PAN: Until I figure out how to properly deal with 
#                 computing dates for both water years and
#                 calender years I'll have to have two versions
#                 of the dparse routine.


def dparse_wy(*dstr):
    # The water year version of dparse is only intended for working
    # with annual data. The monthly and daily dates are always
    # based on a calender year.
    dint = [int(x) for x in dstr]

    if len(dint) == 1:
        # For annual we want the last day of the year
        dint.append(9)
        dint.append(calendar.monthrange(*dint)[1])

    return datetime.datetime(*dint)


def dparse(*dstr):
    dint = [int(x) for x in dstr]

    if len(dint) == 2:
        # For months we want the last day of each month
        dint.append(calendar.monthrange(*dint)[1])
    if len(dint) == 1:
        # For annual we want the last day of the year
        dint.append(12)
        dint.append(calendar.monthrange(*dint)[1])

    return datetime.datetime(*dint)


def wyr_to_datetime(yr):
    """This function takes a water year and returns a datetime object
     of the last day of that water year"""
    dt = datetime.datetime(yr, 9, 30)
    return dt


def main():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Compute Kendall tau from NWIS annual streamflow observations')
    parser.add_argument('obsfile', help='NWIS annual streamflow filename')
    parser.add_argument('stnfile', help='NWIS streamgage information filename')
    parser.add_argument('outfile', help='Output filename prefix for obs and stats')
    parser.add_argument('-w', '--wateryears', help='Observation dates are based on water years', action='store_true')
    parser.add_argument('-d', '--daterange',
                        help='Starting and ending calendar date (YYYY-MM-DD YYYY-MM-DD)',
                        nargs=2, metavar=('startDate', 'endDate'), required=True)
    parser.add_argument('-p', '--pval', help='Maximum p-value', type=float, required=True)
    parser.add_argument('-O', '--overwrite', help='Overwrite existing output file', action='store_true')

    args = parser.parse_args()

    if not os.path.isfile(args.obsfile):
        print("The streamflow observation file, %s, does not exist" % args.obsfile)
        exit(1)

    if not os.path.isfile(args.stnfile):
        print("The streamgage information file, %s, does not exist" % args.stnfile)
        exit(1)

    if not args.overwrite and os.path.isfile(args.outfile):
        print("Output filename exists. To force overwrite specify -O on command line")
        exit(1)

    # Create a logfile for the work
    logfile = '%s.log' % args.outfile
    loghdl = open(logfile, 'w')
    log_list = []
    log_list.append('='*70)
    log_list.append('Program executed %s' % strftime('%Y-%m-%d %H:%M:%S %z'))
    log_list.append('-'*70)
    log_list.append(" ".join(sys.argv))
    log_list.append('-'*70)
    log_list.append('Script version: %s' % __version__)
    log_list.append('Script directory: %s' % os.path.dirname(os.path.abspath(__file__)))
    log_list.append('Python: %s (%s)' % (platform.python_implementation(), platform.python_version()))
    log_list.append('Host: %s' % platform.node())
    log_list.append('-'*70)
    log_list.append('Current directory: %s' % os.getcwd())
    log_list.append(' Observation file: %s' % args.obsfile)
    log_list.append('Station info file: %s' % args.stnfile)
    log_list.append('      Output file: %s' % args.outfile)

    st = datetime.datetime(*(map(int, args.daterange[0].split('-'))))
    en = datetime.datetime(*(map(int, args.daterange[1].split('-'))))

    # Compute the period of record for this date range
    por = en.year - st.year
    # if not args.wateryears:
    #    por += 1    # For calendar years we need to add one year

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Read in the streamgage information
    stn_col_names = ['site_no', 'station_nm', 'dec_lat_va', 'dec_long_va',
                     'drain_area_va', 'contrib_drain_area_va']
    stn_col_types = [np.str_, np.str_, np.float_, np.float_, np.float_, np.float_]
    stn_cols = dict(zip(stn_col_names, stn_col_types))

    stations = pd.read_csv(args.stnfile, sep='\t', usecols=stn_col_names,
                           dtype=stn_cols)

    stations.set_index('site_no', inplace=True)

    # Have to force numeric conversion after the fact when there are
    # null values in a column
    for dd in stations.columns:
        if stn_cols[dd] == np.float_:
            stations[dd] = pd.to_numeric(stations[dd], errors='coerce')
            # stations[dd] = stations[dd].convert_objects(convert_numeric='force')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Import the streamflow data. Use a custom date parser to create datetime values
    # at the end of each water year.
    # The usecols parameter is assuming that site_no, dd_nu, year_nu, and mean_va
    # are in columns 1,3,4,5, respectively (assuming zero-based indexing).
    # agency_cd	site_no	parameter_cd	ts_id	loc_web_ds	year_nu	mean_va
    obs_col_names = ['site_no', 'ts_id', 'year_nu', 'mean_va']
    obs_col_types = [np.str_, np.int16, np.int16, np.float64]
    obs_cols = dict(zip(obs_col_names, obs_col_types))

    # thedata = pd.read_csv(args.obsfile, sep='\t', usecols=obs_col_names, dtype=obs_cols)
    if args.wateryears:
        # We use a different version of the dparse routine when
        # working with observation dates (years) given in water years.
        thedata = pd.read_csv(args.obsfile, sep='\t', usecols=obs_col_names, dtype=obs_cols,
                              date_parser=dparse_wy, parse_dates={'wyear': ['year_nu']},
                              index_col='wyear', keep_date_col=True)
    else:
        thedata = pd.read_csv(args.obsfile, sep='\t', usecols=obs_col_names, dtype=obs_cols,
                              date_parser=dparse, parse_dates={'wyear': ['year_nu']},
                              index_col='wyear', keep_date_col=True)

    # Have to force numeric conversion after the fact when there are
    # null values in a column
    # for dd in thedata.columns:
    #    print dd, thedata[dd].dtype

    # Select only the observations that are within our period of interest
    thedata = thedata[(thedata.index >= st) & (thedata.index <= en)]

    # Group by site_no and dd_nu. Filter by sites that don't have enough observations
    # in the period of interest
    thedata = thedata.groupby(['site_no', 'ts_id']).filter(lambda x: len(x) >= por)

    # ------------------------------------------------------------------------
    # Write out the annual observations
    thedata.reset_index().to_csv('%s_obs.tab' % args.outfile, sep='\t', index=False,
                                 header=['siteno', 'date', 'waterYr', 'avgQ'],
                                 float_format='%.3f',
                                 columns=['site_no', 'wyear', 'year_nu', 'mean_va'])

    # Pivot the table so waterYr is the row index and each site is a column
    sitedataByCol = thedata.reset_index().pivot(index='wyear', columns='site_no', values='mean_va')

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Create a sequence of numbers same total size as npdata
    thetime = sitedataByCol.index.to_julian_date().values

    # Create a dictionary for the results
    outdata = {'site_no': [], 'pval': [], 'tau': [], 'trend': []}
    rescount = Counter()    # counters for summary of results
    # rescount = {'total':0, 'up':0, 'down':0}    # counters for summary of results

    # Loop through each column and compute Kendall Tau
    for tt in sitedataByCol.columns:
        rescount['total'] += 1
        npdata = sitedataByCol[tt].values

        # Compute Kendall tau for this site
        # result indices: tau,0; svar,1; z,2; pval,3
        result = nr3.kendall_numpy(thetime, npdata)

        # Add results to the outdata dictionary
        outdata['site_no'].append(tt)
        outdata['pval'].append(result[3])
        outdata['tau'].append(result[0])

        if result[3] <= args.pval:
            if result[0] < 0:
                outdata['trend'].append(-1)
                rescount['down'] += 1
            elif result[0] > 0:
                outdata['trend'].append(1)
                rescount['up'] += 1
            else:
                outdata['trend'].append(0)
        else:
            # Mark non-significant trends so GIS can handle the symbology easier
            if result[0] < 0:
                outdata['trend'].append(-2)
            elif result[0] > 0:
                outdata['trend'].append(2)
            else:
                outdata['trend'].append(0)

    log_list.append('-'*70)

    log_list.append(f'Trends summary (total/up/down): {args.outfile},{rescount["total"]},{rescount["up"]},{rescount["down"]}')
    # log_list.append(' Total stations: %d' % rescount['total'])
    # log_list.append('  Upward trends: %d' % rescount['up'])
    # log_list.append('Downward trends: %d' % rescount['down'])
    log_list.append('='*70)

    # Convert dictionary to a dataframe
    testdf = pd.DataFrame(outdata, columns=['site_no', 'pval', 'tau', 'trend'])
    testdf.set_index('site_no', inplace=True)

    # Merge the site information with the trend results
    # merged_df = pd.merge(testdf, stations, on='site_no', how='left')
    merged_df = pd.merge(stations, testdf, left_index=True, right_index=True, how='right')

    # Compute a few statistics
    lastten = sitedataByCol.last('10Y').mean().rename('last_ten_yr')
    firstten = sitedataByCol.first('10Y').mean().rename('first_ten_yr')
    pct_chg = ((lastten - firstten) / firstten).rename('pct_chg')
    df_stats = pd.concat([firstten, lastten, pct_chg], axis=1)

    merged_df = pd.merge(merged_df, df_stats, left_index=True, right_index=True, how='left')
    # Write the dataframe out to a csv file
    merged_df.to_csv('%s_kendall.tab' % args.outfile, sep='\t', float_format='%1.5f', header=True, index=True)

    # Write the log file
    for xx in log_list:
        print(xx)
        loghdl.write(xx + '\n')
    loghdl.close()
    print("Summary written to %s" % logfile)


if __name__ == '__main__':
    main()
