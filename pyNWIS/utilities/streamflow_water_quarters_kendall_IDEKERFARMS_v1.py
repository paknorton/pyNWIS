#!/usr/bin/env python
"""This script reads files of downloaded NWIS monthly observations and
station information and computes the Kendall tau for each station
within a specified date range. Results and associated station information
are written to a tab-delimited output file.
"""

#      Author: Parker Norton
#        Date: 2015-10-13
# Description: Reads NWIS data that was pulled using XXXXXX.py
#              and computes the Kendall Tau for each column of data for a subset
#              based on a date range. The Kendal Tau results are then written to
#              a tab-delimited file.

#              2017-02-13 PAN: This is a one-off version of the script to generate
#                              additional streamflow results for the Ideker Farms
#                              litigation. The script has been modified to
#                              work with the older format used for the
#                              datasets in the MRB wy1960-2011 SIR.
#                              No other modifications were made.

from __future__ import (absolute_import, division, print_function)

import os
import platform
import sys
import pandas as pd
import numpy as np
import datetime
import kendall_cy as nr3
# from dateutil.relativedelta import relativedelta
from time import strftime
import argparse
import calendar
from collections import OrderedDict
from collections import Counter

__author__ = 'Parker Norton (pnorton@usgs.gov)'
__version__ = '0.2'

# TODO: This code needs some work. The wateryears argument really isn't used, but the for the quarters is hardcoded
#       for the assumption that these are quarters in water years. There needs to be better flexibility.

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

# Command line arguments
parser = argparse.ArgumentParser(description='Compute Kendall tau from NWIS annual streamflow observations')
parser.add_argument('obsfile', help='NWIS annual streamflow filename')
parser.add_argument('stnfile', help='NWIS streamgage information filename')
parser.add_argument('outfile', help='Output filename prefix for obs and stats')
parser.add_argument('-w', '--wateryears', help='Observations are stored by water years', action='store_true')
parser.add_argument('-d', '--daterange',
                    help='Starting and ending date (YYYY-MM-DD YYYY-MM-DD)',
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
#logfile = '%s/%s.log' % (os.path.dirname(args.outfile), os.path.splitext(os.path.basename(args.outfile))[0])
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
por = (en.year - st.year) * 12

log_list.append('-'*70)
log_list.append('Start date: %s' % st)
log_list.append('End date: %s' % en)
log_list.append('Water years: %s' % args.wateryears)
log_list.append('Period of record: %d' % por)
log_list.append('Max p-value: %0.2f' % args.pval)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Read in the streamgage information
# stn_col_names = ['site_no', 'station_nm', 'PROV_TERR_STATE_LOC', 'dec_lat_va', 'dec_long_va',
stn_col_names = ['site_no']
                # 'station_nm', 'dec_lat_va', 'dec_long_va',
                #  'drain_area_va',
                 # 'coord_datum_cd', 'alt_va', 'huc_cd', 'drain_area_va',
                 # 'contrib_drain_area_va']
stn_col_types = [np.str_]
                # np.str_, np.float_, np.float_, np.str_, np.float_,
                #  np.str_, np.float_, np.float_]
stn_cols = dict(zip(stn_col_names, stn_col_types))

stations = pd.read_csv(args.stnfile, sep='\t', usecols=stn_col_names, dtype=np.str_)

# Have to force numeric conversion after the fact when there are
# null values in a column
# for dd in stations.columns:
#     if stn_cols[dd] == np.float_:
#         stations[dd] = stations[dd].convert_objects(convert_numeric='force')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Import the streamflow data. Use a custom date parser to create datetime values
# at the end of each water year.
# The usecols parameter is assuming that site_no, dd_nu, year_nu, and mean_va
# are in columns 1,3,4,5, respectively (assuming zero-based indexing).
obs_col_names = ['site_no', 'dd_nu', 'year_nu', 'month_nu', 'mean_va']
# obs_col_names = ['site_no', 'ts_id', 'year_nu', 'month_nu', 'mean_va']
obs_col_types = [np.str_, np.int16, np.int16, np.int16, np.float64]
obs_cols = dict(zip(obs_col_names, obs_col_types))

thedata = pd.read_csv(args.obsfile, sep='\t', usecols=obs_col_names, dtype=obs_cols,
                      date_parser=dparse, parse_dates={'thedate': ['year_nu', 'month_nu']},
                      index_col='thedate')

# Select only the observations that are within our period of interest
thedata = thedata[(thedata.index >= st) & (thedata.index <= en)]

# Group by site_no and dd_nu. Filter by sites that don't have enough observations
# in the period of interest
thedata = thedata.groupby(['site_no', 'dd_nu']).filter(lambda x: len(x) >= por)

# Pivot the table so thedate is the row index and each site is a column
sitedataByCol = thedata.reset_index().pivot(index='thedate', columns='site_no', values='mean_va')

sitedata_wq1 = sitedataByCol.resample('Q-SEP', how='mean')


# ------------------------------------------------------------------------
# First, create copy of dataframe for outputting the observations to a csv
sitedata_wq_obs = pd.DataFrame(sitedata_wq1.unstack().reset_index().copy())

# Adjust the column names and add name for last column (avgQ)
sitedata_wq_obs.rename(columns={'site_no': 'siteno', 'thedate': 'thedate', 0: 'avgQ'}, inplace=True)

# Add fields for water year and water quarter
aa = pd.DatetimeIndex(pd.to_datetime(sitedata_wq_obs['thedate']))
sitedata_wq_obs['waterYr'] = aa.year
sitedata_wq_obs['wQtr'] = aa.quarter

sitedata_wq_obs['waterYr'] = np.where(sitedata_wq_obs['wQtr'] == 4,
                                        sitedata_wq_obs['waterYr']+1, sitedata_wq_obs['waterYr'])
sitedata_wq_obs['wQtr'] = np.where(sitedata_wq_obs['wQtr'] < 4,
                                   sitedata_wq_obs['wQtr']+1, 1)

# Add informational period field
sitedata_wq_obs['period'] = 'WY%d to WY%d; p-val = %.2f' % (sitedata_wq_obs['waterYr'].min(), 
                                                          sitedata_wq_obs['waterYr'].max(),
                                                          args.pval)

sitedata_wq_obs.to_csv('%s_obs.tab' % args.outfile, sep='\t', header=True, index=False,
                       float_format='%.3f',
                       columns=['siteno', 'waterYr', 'wQtr', 'avgQ', 'period'])
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ------------------------------------------------------------------------
# Now create fields for water quarter year and water quarter number (1-4)
# for sitedata_wq1 used to compute statistics
sitedata_wq1['qyear'] = sitedata_wq1.index.year
sitedata_wq1['qtr'] = sitedata_wq1.index.quarter

sitedata_wq1['qyear'] = np.where(sitedata_wq1['qtr'] == 4,
                                 sitedata_wq1['qyear']+1, sitedata_wq1['qyear'])
sitedata_wq1['qtr'] = np.where(sitedata_wq1['qtr'] < 4,
                               sitedata_wq1['qtr']+1, 1)

outresult = {}
for qq in [1, 2, 3, 4]:
    tmp1 = sitedata_wq1[sitedata_wq1['qtr'] == qq]
    thetime = tmp1.index.to_julian_date().values

    for ss in sitedataByCol.columns:
        tmp2 = tmp1[ss].values

        result = nr3.kendall_numpy(thetime, tmp2)

        if ss not in outresult:
            outresult[ss] = []
        outresult[ss].append(result)

# Create a dictionary for the results
outdata = {'site_no': [], 'wQtr': [], 'pval': [], 'tau': [], 'trend': []}
qrescount = OrderedDict()

for ss, kk in outresult.iteritems():
    for ii, qq in enumerate(kk):

        # result indices: tau,0; svar,1; z,2; pval,3

        # Add results to the outdata dictionary
        outdata['site_no'].append(ss)
        outdata['wQtr'].append(ii+1)
        outdata['pval'].append(qq[3])
        outdata['tau'].append(qq[0])

        cntkey = '%d' % (ii+1)

        if cntkey not in qrescount:
            qrescount[cntkey] = Counter()
        
        qrescount[cntkey]['total'] += 1

        if qq[3] <= args.pval:
            if qq[0] < 0:
                outdata['trend'].append(-1)
                qrescount[cntkey]['down'] += 1
            elif qq[0] > 0:
                outdata['trend'].append(1)
                qrescount[cntkey]['up'] += 1
            else:
                outdata['trend'].append(0)
        else:
            outdata['trend'].append(0)

# Convert dictionary to a dataframe
testdf = pd.DataFrame(outdata, columns=['site_no', 'wQtr', 'pval', 'tau', 'trend'])

# Merge the site information with the trend results
merged_df = pd.merge(testdf, stations, on='site_no', how='left')

# Write the dataframe out to a csv file
merged_df.to_csv('%s_kendall.tab' % args.outfile, sep='\t', float_format='%1.5f', header=True, index=False)

log_list.append('\n======= Summary =======')
for kk, vv in qrescount.iteritems():
    log_list.append('----- Quarter %s -----' % kk)
    log_list.append(' Total stations: %d' % vv['total'])
    log_list.append('  Upward trends: %d' % vv['up'])
    log_list.append('Downward trends: %d' % vv['down'])
log_list.append('='*30)

# Write the log file
for xx in log_list:
    print(xx)
    loghdl.write(xx + '\n')
loghdl.close()
print("Summary written to %s" % logfile)
