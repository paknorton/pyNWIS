#!/usr/bin/env python3

"""This script downloads peakflow streamflow observations from
the NWIS REST and Water Data services for a given Hydrologic Unit Code.
Two data files are output
1) the peakflow observations and 2) the streamgage information."""

#      Author: Parker Norton (pnorton@usgs.gov)
#        Date: 2016-02-2
# Description: Downloads peak streamflow observations from
#              NWIS for a given Hydrologic Unit Code.

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
parser.add_argument('-O', '--overwrite', help='Overwrite existing output file', action='store_true')
parser.add_argument('-R', '--region', help='Hydrologic Unit Code for stations to select')

args = parser.parse_args()

# Additional parts to add to output filenames
addin = f'HUC_{args.region}'

# Construct the filenames for streamgage observations, streamgage information, and the log file
dirpart = os.path.dirname(args.outfile)
nameparts = os.path.splitext(os.path.basename(args.outfile))
obsfile = os.path.join(dirpart, f'{nameparts[0]}_{addin}_obs{nameparts[1]}')
stnfile = os.path.join(dirpart, f'{nameparts[0]}_{addin}_stn{nameparts[1]}')
logfile = os.path.join(dirpart, f'{nameparts[0]}_{addin}.log')

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
logging.info(' '.join(sys.argv))
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

# TODO: sometimes the URLs can change and a redirect is returned when trying to download
#       data. We need to intercept this, print a warning and then use the new url for
#       downloads.
base_url = 'https://waterservices.usgs.gov/nwis'

# Peak values are currently only available through waterdata
base_waterdata_url = 'https://nwis.waterdata.usgs.gov/usa/nwis'

# NOTE: Cannot use siteOutput=expanded for peakflow sites
stn_pieces = OrderedDict()
stn_pieces['format'] = 'rdb'
stn_pieces['huc'] = f'{args.region}'
stn_pieces['siteStatus'] = 'active'    # One of: all, active, inactive
stn_pieces['outputDataTypeCd'] = 'pk'
stn_pieces['hasDataTypeCd'] = 'pk'
stn_final = '&'.join([f'{kk}={vv}' for kk, vv in stn_pieces.items()])

stn_url = f'{base_url}/site/?{stn_final}'

# stn_url = f'{base_url}/site/?format=rdb&huc={args.region}&siteStatus=active&outputDataTypeCd=pk'

logging.info(f'Region: {args.region}')
logging.info(f'Report type: peakflows')
logging.info('-'*70)
logging.info(f'Base URL: {base_url}')
logging.info(f'Water Data URL: {base_waterdata_url}')
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

        obs_url = f'{base_waterdata_url}/peak/?format=rdb&site_no={ff[fld["site_no"]]}&'
        # obs_url = '{0:s}/peak/?format=rdb&site_no={1:s}&'.format(base_waterdata_url, ff[fld['site_no']])
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
                # waterdata site returns stupid dos line endings - strip them out
                obs_hdl.write(obs.strip('\r') + '\n')

        # Write the streamgage information file
        stn_hdl.write(cStreamgage + '\n')
        sys.stdout.write('\r' + ' '*60 + '\r')
stn_hdl.close()
obs_hdl.close()

print(f'Summary written to {logfile}')
