#!/usr/bin/env python3

"""This script downloads annual, monthly, or daily streamflow observations from
the NWIS REST service for a given Hydrologic Unit Code.
Two data files are output
1) the streamflow observations and 2) the streamgage information."""

#      Author: Parker Norton (pnorton@usgs.gov)
#        Date: 2014-09-09
# Description: Downloads annual, monthly, or daily streamflow observations from
#              the NWIS REST service for a given Hydrologic Unit Code.

import os
import platform
import sys
import re
from time import strftime
import argparse
import logging

from collections import OrderedDict
from urllib.request import urlopen   # , Request
# from urllib.error import HTTPError

__version__ = '0.2'
__author__ = 'Parker Norton (pnorton@usgs.gov)'

# Command line arguments
parser = argparse.ArgumentParser(description='Download streamflow observations from NWIS REST service.')
parser.add_argument('outfile', help='Output filename base (e.g. nwis.tab)')
parser.add_argument('-w', '--wateryears', help='Observations are stored by water years', action='store_true')
parser.add_argument('-d', '--daterange',
                    help='Starting and ending calendar date (YYYY-MM-DD YYYY-MM-DD)',
                    nargs=2, metavar=('startDate', 'endDate'), required=True)
parser.add_argument('-t', '--statRepType', help='Statistic report type',
                    choices=['annual', 'monthly', 'daily'], default='annual')
parser.add_argument('-s', '--stat', help='Type of statistic', choices=['mean'], default='mean')
parser.add_argument('-O', '--overwrite', help='Overwrite existing output file', action='store_true')
parser.add_argument('-R', '--region', help='Hydrologic Unit Code for stations to select')
parser.add_argument('--show_restricted', help='Retrieved parameters/values that have access restrictions',
                    action='store_true')

args = parser.parse_args()

# Additional parts to add to output filenames
addin = '{0:s}_HUC_{1:s}'.format(args.statRepType, args.region)

# Specifying observations by water year is only legal for annual report types
if args.statRepType == 'annual' and args.wateryears:
    # statYearType = '&statYearType=water'
    addin += '_WY'
# else:
#     statYearType = ''

# Construct the filenames for streamgage observations, streamgage information, and the log file
dirpart = os.path.dirname(args.outfile)
nameparts = os.path.splitext(os.path.basename(args.outfile))
obsfile = os.path.join(dirpart, '{0:s}_{1:s}_obs{2:s}'.format(nameparts[0], addin, nameparts[1]))
stnfile = os.path.join(dirpart, '{0:s}_{1:s}_stn{2:s}'.format(nameparts[0], addin, nameparts[1]))
logfile = os.path.join(dirpart, '{0:s}_{1:s}.log'.format(nameparts[0], addin))

print(f'Streamgage observation file: {obsfile}')
print(f'Streamgage information file: {stnfile}')
print(f'Session log file: {logfile}')

if not args.overwrite and os.path.isfile(stnfile):
    print(f'The streamflow information file, {stnfile}, already exists.\nTo force overwrite specify -O on command line')
    exit(1)

if not args.overwrite and os.path.isfile(obsfile):
    print(f'The streamflow observation file, {obsfile}, already exists.\nTo force overwrite specify -O on command line')
    exit(1)

# Open logfile and start collecting basic information to write out later
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(levelname)s:%(asctime)s:%(message)s')

logging.info(f'Program executed {strftime("%Y-%m-%d %H:%M:%S %z")}')
logging.info(" ".join(sys.argv))
logging.info(f'Script version: {__version__}')
logging.info(f'Script directory: {os.path.dirname(os.path.abspath(__file__))}')
logging.info(f'Python: {platform.python_implementation()} ({platform.python_version()})')
logging.info(f'Host: {platform.node()}')
logging.info('-'*70)
logging.info(f'Current directory: {os.getcwd()}')
logging.info(f' Observation file: {obsfile}')
logging.info(f'Station info file: {stnfile}')
logging.info(f'         Log file: {logfile}')

# URLs can be generated/tested at: http://waterservices.usgs.gov/rest/Site-Test-Tool.html
base_url = 'https://waterservices.usgs.gov/nwis'

stn_pieces = OrderedDict()
stn_pieces['format'] = 'rdb'
stn_pieces['huc'] = f'{args.region}'
stn_pieces['siteOutput'] = 'expanded'
stn_pieces['siteStatus'] = 'all'
stn_pieces['parameterCd'] = '00060'
stn_pieces['siteType'] = 'ST'
stn_final = '&'.join([f'{kk}={vv}' for kk, vv in stn_pieces.items()])

stn_url = f'{base_url}/site/?{stn_final}'

logging.info(f'Water years: {str(args.statRepType == "annual" and args.wateryears)}')
logging.info(f'Region: {args.region}')
logging.info(f'Report type: {args.statRepType}')
logging.info(f'Statistic type: {args.stat}')
logging.info('-'*70)
logging.info(f'Base URL: {base_url}')
logging.info(f'Station URL: {stn_url}')

# Open station and observation files
stn_hdl = open(stnfile, "w")
obs_hdl = open(obsfile, "w")

t1 = re.compile('^#.*$\n?', re.MULTILINE)   # remove comment lines
t2 = re.compile('^5s.*$\n?', re.MULTILINE)  # remove field length lines

# Retrieve stations from NWIS site service
response = urlopen(stn_url)
encoding = response.info().get_param('charset', failobj='utf8')
streamgage_site_page = response.read().decode(encoding)

# Strip the comment lines and field length lines from the result
streamgage_site_page = t1.sub('', streamgage_site_page, 0)
streamgage_site_page = t2.sub('', streamgage_site_page, 0)

# Build the non-changing parts of the REST URL for pulling streamflow values
url_pieces = OrderedDict()
url_pieces['format'] = 'rdb'

if args.statRepType == 'monthly':
    url_pieces['startDT'] = args.daterange[0][0:7]
    url_pieces['endDT'] = args.daterange[1][0:7]
else:
    url_pieces['startDT'] = args.daterange[0]
    url_pieces['endDT'] = args.daterange[1]

url_pieces['parameterCd'] = '00060'  # Discharge
url_pieces['statReportType'] = args.statRepType
url_pieces['statType'] = args.stat

if args.statRepType == 'annual' and args.wateryears:
    url_pieces['statYearType'] = 'water'

if args.show_restricted:
    url_pieces['access'] = 3

fld = {}

# Each request gives a new header; we only want one
header_written = False

logging.info('========== Streamgage observation URLs ==========')
# Loop through each site and download the observations
for cStreamgage in streamgage_site_page.split('\n'):
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

        url_pieces['site'] = ff[fld['site_no']]

        url_final = '&'.join([f'{kk}={vv}' for kk, vv in url_pieces.items()])

        obs_url = f'{base_url}/stat/?{url_final}'

        logging.info(obs_url)
        sys.stdout.write(f'\rDownloading observations for streamgage: {ff[fld["site_no"]]}')
        sys.stdout.flush()

        # Read site data
        response = urlopen(obs_url)
        encoding = response.info().get_param('charset', failobj='utf8')
        streamgage_obs_page = response.read().decode(encoding)

        # Strip the comment lines and field length lines from the result using regex
        streamgage_obs_page = t1.sub('', streamgage_obs_page, 0)
        streamgage_obs_page = t2.sub('', streamgage_obs_page, 0)

        # Write the streamgage observations to the output file
        for obs in streamgage_obs_page.split('\n'):
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
        sys.stdout.write('\r' + ' '*60 + '\r')
stn_hdl.close()
obs_hdl.close()

print(f'Summary written to {logfile}')
