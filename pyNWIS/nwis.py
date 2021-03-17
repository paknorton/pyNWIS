
import re

from urllib.request import urlopen, Request
from urllib.error import HTTPError

__author__ = 'Parker Norton (pnorton@usgs.gov)'

# URLs can be generated/tested at: http://waterservices.usgs.gov/rest/Site-Test-Tool.html
BASE_URL = 'http://waterservices.usgs.gov/nwis'

RE_COMMENTS = re.compile('^#.*$\n?', re.MULTILINE)   # remove comment lines
RE_FLD_LENGTH = re.compile('^5s.*$\n?', re.MULTILINE)  # remove field length lines


class NWIS:
    def __init__(self):
        pass

    @staticmethod
    def strip_comments(txt):
        # Strip comment lines from an RDB-formatted string
        return RE_COMMENTS.sub('', txt, 0)

    @staticmethod
    def strip_fld_lengths(txt):
        # Strip field-length lines from an RDB-formatted string
        return RE_FLD_LENGTH.findall(txt)[0].strip('\n').split('\t')

    def get_page(self, url, comments=True, fld_lengths=True):
        # Get a response from NWIS for the given url.
        # By default the returned page is stripped of comments and field-length lines

        response = urlopen(url)
        encoding = response.info().get_param('charset', failobj='utf8')
        returned_page = response.read().decode(encoding)

        if comments:
            # Strip the comment lines and field length lines from the result
            returned_page = self.strip_comments(returned_page)

        if fld_lengths:
            returned_page = self.strip_fld_lengths(returned_page)

        return returned_page
