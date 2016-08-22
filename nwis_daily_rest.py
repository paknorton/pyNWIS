#!/usr/bin/env python2.7
"""This script downloads daily streamflow observations from
the NWIS REST service for a set of streamgages.
Two data files are output
1) the streamflow observations and 2) the streamgage information."""

#      Author: Parker Norton (pnorton@usgs.gov)
#        Date: 2014-12-31
# Description: Downloads or daily streamflow observations from
#              the NWIS REST service for a set of streamgages.

from __future__ import (absolute_import, division, print_function)
# from future.utils import iteritems, itervalues

import os
import platform
import sys
import re
from time import strftime
import argparse
import logging

# The urllib library is named differently for Python 2 and 3
try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

__version__ = '0.1'

# Command line arguments
parser = argparse.ArgumentParser(description='Download streamflow observations from NWIS REST service.')
parser.add_argument('outfile', help='Output filename base (e.g. nwis.tab)')
# parser.add_argument('-w', '--wateryears', help='Observations are stored by water years', action='store_true')
# parser.add_argument('-d', '--daterange',
#                     help='Starting and ending date (YYYY-MM-DD YYYY-MM-DD)',
#                     nargs=2, metavar=('startDate','endDate'), required=True)
parser.add_argument('-t', '--statRepType', help='Statistic report type',
                    choices=['annual', 'monthly', 'daily'], default='annual')
parser.add_argument('-s', '--stat', help='Type of statistic', choices=['mean'], default='mean')
parser.add_argument('-O', '--overwrite', help='Overwrite existing output file', action='store_true')
parser.add_argument('-R', '--region', help='Hydrologic Unit Code for stations to select')

args = parser.parse_args()

# List of streamgages for download
sglist = ['05536890', '05536995', '05537980', '05527500', '05543010', '05543500']
sitelist = ','.join(sglist)

# List of parameter codes to retrieve
plist = ['00095', '00300', '00400', '63680', '99133']
paramList = ','.join(plist)

# Additional parts to add to output filenames
addin = '{0:s}_region_{1:s}'.format(args.statRepType, args.region)

# Specifying observations by water year is only legal for annual report types
if args.statRepType == 'annual' and args.wateryears:
    statYearType = '&statYearType=water'
    addin += '_WY'
else:
    statYearType = ''

# Construct the filenames for streamgage observations, streamgage information, and the log file
dirpart = os.path.dirname(args.outfile)
nameparts = os.path.splitext(os.path.basename(args.outfile))
obsfile = os.path.join(dirpart, '{0:s}_{1:s}_obs{2:s}'.format(nameparts[0], addin, nameparts[1]))
stnfile = os.path.join(dirpart, '{0:s}_{1:s}_stn{2:s}'.format(nameparts[0], addin, nameparts[1]))
logfile = os.path.join(dirpart, '{0:s}_{1:s}.log'.format(nameparts[0], addin))

print('Streamgage observation file: {0:s}'.format(obsfile))
print('Streamgage information file: {0:s}'.format(stnfile))
print('Session log file: {0:s}'.format(logfile))

if not args.overwrite and os.path.isfile(stnfile):
    print(
        "The streamflow information file, {0:s}, already exists.\nTo force overwrite specify -O on command line".format(
            stnfile))
    exit(1)

if not args.overwrite and os.path.isfile(obsfile):
    print(
        "The streamflow observation file, {0:s}, already exists.\nTo force overwrite specify -O on command line".format(
            obsfile))
    exit(1)

# Open logfile and start collecting basic information to write out later
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(levelname)s:%(asctime)s:%(message)s')

logging.info('Program executed {0:s}'.format(strftime('%Y-%m-%d %H:%M:%S %z')))
logging.info(" ".join(sys.argv))
logging.info('Script version: {0:s}'.format(__version__))
logging.info('Script directory: {0:s}'.format(os.path.dirname(os.path.abspath(__file__))))
logging.info('Python: {0:s} ({1:s})'.format(platform.python_implementation(), platform.python_version()))
logging.info('Host: {0:s}'.format(platform.node()))
logging.info('-'*70)
logging.info('Current directory: {0:s}'.format(os.getcwd()))
logging.info(' Observation file: {0:s}'.format(obsfile))
logging.info('Station info file: {0:s}'.format(stnfile))
logging.info('         Log file: {0:s}'.format(logfile))

# URLs can be generated/tested at: http://waterservices.usgs.gov/rest/Site-Test-Tool.html
base_url = 'http://waterservices.usgs.gov/nwis'

stn_url = '{0:s}/site/?format=rdb&sites={1:s}&siteOutput=expanded&siteStatus=active&parameterCd={2:s}&siteType=ST' \
    .format(base_url, sitelist, paramList)

logging.info('Water years: {0:s}'.format(args.statRepType == 'annual' and args.wateryears))
logging.info('Sites: {0:s}'.format(sitelist))
logging.info('Report type: {0:s}'.format(args.statRepType))
logging.info('Statistic type: {0:s}'.format(args.stat))
logging.info('-'*70)
logging.info('Base URL: {0:s}'.format(base_url))
logging.info('Station URL: {0:s}'.format(stn_url))

# Open NWIS REST site service
streamgagesPage = urlopen(stn_url)

# Open station and observation files
stn_hdl = open(stnfile, "w")
obs_hdl = open(obsfile, "w")

t1 = re.compile('^#.*$\n?', re.MULTILINE)   # remove comment lines
t2 = re.compile('^5s.*$\n?', re.MULTILINE)  # remove field length lines

# Get the list of streamgages within the specified region
streamgagesFromREST = streamgagesPage.read()

# Strip the comment lines and field length lines from the result
streamgagesFromREST = t1.sub('', streamgagesFromREST, 0)
streamgagesFromREST = t2.sub('', streamgagesFromREST, 0)

fld = {}
print(streamgagesFromREST)
exit()

# Each request gives a new header; we only want one
header_written = False

logging.info('========== Streamgage observation URLs ==========')
# Loop through each site and download the observations
for cStreamgage in streamgagesFromREST.split('\n'):
    if len(cStreamgage) > 0:
        ff = cStreamgage.split('\t')
        if ff[0] == 'agency_cd':
            # Get the fieldnames for the site information
            cc = 0
            for sf in ff:
                # Build a list of indices to each field name
                fld[sf] = cc
                cc += 1
            stn_hdl.write(cStreamgage + '\n')
            continue

        obs_url = '{0:s}/stat/?format=rdb&site={1:s}&statReportType={2:s}&statType={3:s}{4:s}&parameterCd=00060' \
            .format(base_url, ff[fld['site_no']], args.statRepType, args.stat, statYearType)
        logging.info(obs_url)
        print("Downloading observations for streamgage: {0:s}".format(ff[fld['site_no']]))

        # Read site data
        streamgageObsPage = urlopen(obs_url)
        streamgageObservations = streamgageObsPage.read()

        # Strip the comment lines and field length lines from the result using regex
        streamgageObservations = t1.sub('', streamgageObservations, 0)
        streamgageObservations = t2.sub('', streamgageObservations, 0)

        # Write the streamgage observations to the output file
        for obs in streamgageObservations.split('\n'):
            if obs.split('\t')[0] == 'agency_cd':
                # We only want a single header in the output file
                if header_written:
                    continue
                else:
                    header_written = True
            if len(obs) > 0:
                obs_hdl.write(obs + '\n')

        # Write the streamgage information file
        stn_hdl.write(cStreamgage + '\n')
stn_hdl.close()
obs_hdl.close()

print("Summary written to {0:s}".format(logfile))
