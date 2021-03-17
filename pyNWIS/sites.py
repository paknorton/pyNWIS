
import numpy as np
import pandas as pd

from collections import OrderedDict
from io import StringIO
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from pyNWIS.nwis import NWIS, BASE_URL, RE_COMMENTS, RE_FLD_LENGTH

__author__ = 'Parker Norton (pnorton@usgs.gov)'


class Sites(NWIS):

    def __init__(self):
        super().__init__()

    def _get_nwis_site_fields(self):
        # Retrieve a single station and pull out the field names and data types
        url_pieces = OrderedDict()
        url_pieces['format'] = 'rdb'
        url_pieces['sites'] = '01646500'
        url_pieces['siteOutput'] = 'expanded'
        url_pieces['siteStatus'] = 'all'
        url_pieces['parameterCd'] = '00060'  # Discharge
        url_pieces['siteType'] = 'ST'

        url_final = '&'.join([f'{kk}={vv}' for kk, vv in url_pieces.items()])

        stn_url = f'{BASE_URL}/site/?{url_final}'

        streamgage_site_page = self.get_page(stn_url, fld_lengths=False)

        nwis_dtypes = self.strip_fld_lengths(streamgage_site_page)
        nwis_fields = StringIO(streamgage_site_page).getvalue().split('\n')[0].split('\t')

        nwis_final = {}
        for fld in nwis_fields:
            code = fld[-2:]
            if code in ['cd', 'no', 'nm', 'dt']:
                nwis_final[fld] = np.str_
            elif code in ['va']:
                nwis_final[fld] = np.float32
            else:
                nwis_final[fld] = np.str_

        return nwis_final

    def get_nwis_sites(self, stdate, endate, sites=None, regions=None):
        cols = self._get_nwis_site_fields()

        # Columns to include in the final dataframe
        include_cols = ['agency_cd', 'site_no', 'station_nm', 'dec_lat_va', 'dec_long_va', 'dec_coord_datum_cd',
                        'alt_va', 'alt_datum_cd', 'huc_cd', 'drain_area_va', 'contrib_drain_area_va']

        # Start with an empty dataframe
        nwis_sites = pd.DataFrame(columns=include_cols)

        url_pieces = OrderedDict()
        url_pieces['format'] = 'rdb'
        url_pieces['startDT'] = stdate.strftime('%Y-%m-%d')
        url_pieces['endDT'] = endate.strftime('%Y-%m-%d')
        url_pieces['siteOutput'] = 'expanded'
        url_pieces['siteStatus'] = 'all'
        url_pieces['parameterCd'] = '00060'  # Discharge
        url_pieces['siteType'] = 'ST'
        url_pieces['hasDataTypeCd'] = 'dv'

        # NOTE: If both sites and regions parameters are specified the sites
        #       parameter takes precedence.
        if sites is None:
            # No sites specified so default to HUC02-based retrieval
            url_pieces['huc'] = None

            if regions is None:
                # Default to HUC02 regions 1 thru 18
                regions = list(range(1, 19))
            if isinstance(regions, (list, tuple)):
                pass
            else:
                # Single region
                regions = [regions]
        else:
            # One or more sites with specified
            url_pieces['sites'] = None

            if isinstance(sites, (list, tuple)):
                pass
            else:
                # Single string, convert to list of sites
                sites = [sites]

        if 'huc' in url_pieces:
            # for region in range(19):
            for region in regions:
                sys.stdout.write(f'\r  Region: {region:02}')
                sys.stdout.flush()

                url_pieces['huc'] = f'{region:02}'
                url_final = '&'.join([f'{kk}={vv}' for kk, vv in url_pieces.items()])

                # stn_url = f'{base_url}/site/?format=rdb&huc={region+1:02}&siteOutput=expanded&siteStatus=all&parameterCd=00060&siteType=ST'
                stn_url = f'{base_url}/site/?{url_final}'

                streamgage_site_page = _retrieve_from_NWIS(stn_url)

                # Read the rdb file into a dataframe
                df = pd.read_csv(StringIO(streamgage_site_page), sep='\t', dtype=cols, usecols=include_cols)

                nwis_sites = nwis_sites.append(df, ignore_index=True)
                sys.stdout.write('\r                      \r')
        else:
            for site in sites:
                sys.stdout.write(f'\r  Site: {site} ')
                sys.stdout.flush()

                url_pieces['sites'] = site
                url_final = '&'.join([f'{kk}={vv}' for kk, vv in url_pieces.items()])

                stn_url = f'{base_url}/site/?{url_final}'

                try:
                    streamgage_site_page = _retrieve_from_NWIS(stn_url)

                    # Read the rdb file into a dataframe
                    df = pd.read_csv(StringIO(streamgage_site_page), sep='\t', dtype=cols, usecols=include_cols)

                    nwis_sites = nwis_sites.append(df, ignore_index=True)
                except (HTTPError) as err:
                    if err.code == 404:
                        sys.stdout.write(f'HTTPError: {err.code}, site does not meet criteria - SKIPPED\n')
                sys.stdout.write('\r                      \r')

        field_map = {'agency_cd': 'poi_agency',
                     'site_no': 'poi_id',
                     'station_nm': 'poi_name',
                     'dec_lat_va': 'latitude',
                     'dec_long_va': 'longitude',
                     'alt_va': 'elevation',
                     'drain_area_va': 'drainage_area',
                     'contrib_drain_area_va': 'drainage_area_contrib'}

        nwis_sites.rename(columns=field_map, inplace=True)
        nwis_sites.set_index('poi_id', inplace=True)

        return nwis_sites
