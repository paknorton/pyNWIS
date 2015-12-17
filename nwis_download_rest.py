#!/usr/bin/env python2.7
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
import urllib2
import re
from time import strftime
import argparse
import logging


__version__ = '0.1'

# Command line arguments
parser = argparse.ArgumentParser(description='Download streamflow observations from NWIS REST service.')
parser.add_argument('outfile', help='Output filename base (e.g. nwis.tab)')
parser.add_argument('-w', '--wateryears', help='Observations are stored by water years', action='store_true')
#parser.add_argument('-d', '--daterange',
#                    help='Starting and ending date (YYYY-MM-DD YYYY-MM-DD)',
#                    nargs=2, metavar=('startDate','endDate'), required=True)
parser.add_argument('-t', '--statRepType', help='Statistic report type',
                    choices=['annual','monthly','daily'], default='annual')
parser.add_argument('-s', '--stat', help='Type of statistic', choices=['mean'], default='mean')
parser.add_argument('-O', '--overwrite', help='Overwrite existing output file', action='store_true')
parser.add_argument('-R', '--region', help='Hydrologic Unit Code for stations to select')

args = parser.parse_args()

# Additional parts to add to output filenames
addin = '%s_HUC_%s' % (args.statRepType, args.region)

# Specifying observations by water year is only legal for annual report types
if args.statRepType == 'annual' and args.wateryears:
    statYearType = '&statYearType=water'
    addin += '_WY'
else:
    statYearType = ''

# Construct the filenames for streamgage observations, streamgage information, and the log file
dirpart = os.path.dirname(args.outfile)
nameparts = os.path.splitext(os.path.basename(args.outfile))
obsfile = os.path.join(dirpart, '%s_%s_obs%s' % (nameparts[0], addin, nameparts[1]))
stnfile = os.path.join(dirpart, '%s_%s_stn%s' % (nameparts[0], addin, nameparts[1]))
logfile = os.path.join(dirpart, '%s_%s.log' % (nameparts[0], addin))

print 'Streamgage observation file: %s' % obsfile
print 'Streamgage information file: %s' % stnfile
print 'Session log file: %s' % logfile

if not args.overwrite and os.path.isfile(stnfile):
    print "The streamflow information file, %s, already exists.\nTo force overwrite specify -O on command line" % stnfile
    exit(1)

if not args.overwrite and os.path.isfile(obsfile):
    print "The streamflow observation file, %s, already exists.\nTo force overwrite specify -O on command line" % obsfile
    exit(1)

# Open logfile and start collecting basic information to write out later
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(levelname)s:%(asctime)s:%(message)s')

logging.info('Program executed %s' % strftime('%Y-%m-%d %H:%M:%S %z'))
logging.info(" ".join(sys.argv))
logging.info('Script version: %s' % __version__)
logging.info('Script directory: %s' % os.path.dirname(os.path.abspath(__file__)))
logging.info('Python: %s (%s)' % (platform.python_implementation(), platform.python_version()))
logging.info('Host: %s' % platform.node())
logging.info('-'*70)
logging.info('Current directory: %s' % os.getcwd())
logging.info(' Observation file: %s' % obsfile)
logging.info('Station info file: %s' % stnfile)
logging.info('         Log file: %s' % logfile)

# URLs can be generated/tested at: http://waterservices.usgs.gov/rest/Site-Test-Tool.html
base_url = 'http://waterservices.usgs.gov/nwis'

stn_url = '%s/site/?format=rdb&huc=%s&siteOutput=expanded&siteStatus=active&parameterCd=00060&siteType=ST' \
          % (base_url, args.region)

logging.info('Water years: %s' % (args.statRepType == 'annual' and args.wateryears))
logging.info('Region: %s' % args.region)
logging.info('Report type: %s' % args.statRepType)
logging.info('Statistic type: %s' % args.stat)
logging.info('-'*70)
logging.info('Base URL: %s' % base_url)
logging.info('Station URL: %s' % stn_url)

# Open NWIS REST site service
streamgagesPage = urllib2.urlopen(stn_url)

# Open station and observation files
stn_hdl = open(stnfile,"w")
obs_hdl = open(obsfile,"w")

t1 = re.compile('^#.*$\n?', re.MULTILINE)   # remove comment lines
t2 = re.compile('^5s.*$\n?', re.MULTILINE)  # remove field length lines

# Get the list of streamgages within the specified region
streamgagesFromREST = streamgagesPage.read()

# Strip the comment lines and field length lines from the result
streamgagesFromREST = t1.sub('', streamgagesFromREST, 0)
streamgagesFromREST = t2.sub('', streamgagesFromREST, 0)

fld = {}

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

        obs_url = '%s/stat/?format=rdb&site=%s&statReportType=%s&statType=%s%s&parameterCd=00060'\
                  % (base_url, ff[fld['site_no']], args.statRepType, args.stat, statYearType)
        logging.info(obs_url)
        print "Downloading observations for streamgage: %s" % ff[fld['site_no']]

        # Read site data
        streamgageObsPage = urllib2.urlopen(obs_url)
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

print "Summary written to %s" % logfile


