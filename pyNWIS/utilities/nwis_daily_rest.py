#!/usr/bin/env python3
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

from collections import OrderedDict
from urllib.request import urlopen   # , Request
# from urllib.error import HTTPError

__version__ = '0.3'


# stat_choices = ['00001', '00002', '00003', '00008', '00009', '00010', '00013']


def main():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Download streamflow observations from NWIS REST service.')
    parser.add_argument('outfile', help='Output filename base (e.g. nwis.tab)')
    parser.add_argument('-d', '--daterange',
                        help='Starting and ending date (YYYY-MM-DD YYYY-MM-DD)',
                        nargs=2, metavar=('startDate', 'endDate'), required=True)
    parser.add_argument('-O', '--overwrite', help='Overwrite existing output file', action='store_true')
    parser.add_argument('-P', '--parameters', help='Space separated list of parameter codes', nargs='+',
                        default=['00060'], type=str)
    parser.add_argument('-R', '--region', help='Hydrologic Unit Code for stations to select',
                        default=None, type=str)
    parser.add_argument('-s', '--stat', help='List of statistics', nargs='+', default=['00003'])
    parser.add_argument('-S', '--sites', help='Space separated list of streamgages', nargs='+',
                        default=None, type=str)
    parser.add_argument('--show_restricted', help='Retrieved parameters/values that have access restrictions',
                        action='store_true')

    args = parser.parse_args()

    if args.region is not None:
        args.sites = None

    # Additional parts to add to output filenames
    addin = ''
    if args.region is not None:
        addin = f'_region_{args.region}'

    # Construct the filenames for streamgage observations, streamgage information, and the log file
    dirpart = os.path.dirname(args.outfile)
    nameparts = os.path.splitext(os.path.basename(args.outfile))
    obsfile = os.path.join(dirpart, f'{nameparts[0]}{addin}_obs{nameparts[1]}')
    stnfile = os.path.join(dirpart, f'{nameparts[0]}{addin}_stn{nameparts[1]}')
    logfile = os.path.join(dirpart, f'{nameparts[0]}{addin}.log')

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

    if args.region is not None:
        stn_pieces['huc'] = args.region
    else:
        stn_pieces['sites'] = ','.join([ii for ii in args.sites])

    stn_pieces['siteOutput'] = 'expanded'
    stn_pieces['siteStatus'] = 'all'
    stn_pieces['parameterCd'] = ','.join([ii for ii in args.parameters])
    stn_pieces['siteType'] = 'ST'
    stn_final = '&'.join([f'{kk}={vv}' for kk, vv in stn_pieces.items()])

    stn_url = f'{base_url}/site/?{stn_final}'

    logging.info(f'Region: {args.region}')
    logging.info(f'Sites: {args.sites}')
    # logging.info(f'Report type: {args.statRepType}')
    logging.info(f'Parameters: {args.parameters}')
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

    url_pieces['startDT'] = args.daterange[0]
    url_pieces['endDT'] = args.daterange[1]

    url_pieces['parameterCd'] = ','.join([ii for ii in args.parameters])  # Discharge
    # url_pieces['statReportType'] = args.statRepType
    url_pieces['statCd'] = ','.join([ii for ii in args.stat])

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

            obs_url = f'{base_url}/dv/?{url_final}'

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
                    if obs[0] != '\t':
                        # Empty data returns can have all tabs
                        obs_hdl.write(obs + '\n')

            # Write the streamgage information file
            stn_hdl.write(cStreamgage + '\n')
            sys.stdout.write('\r' + ' '*60 + '\r')
    stn_hdl.close()
    obs_hdl.close()

    print(f'Summary written to {logfile}')


if __name__ == '__main__':
    main()
