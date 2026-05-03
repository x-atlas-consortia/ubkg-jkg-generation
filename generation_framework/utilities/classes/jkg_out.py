"""
jkg_out.py
Class to manage common output file names.

"""

from .ubkg_config import ubkgConfigParser
from .ubkg_logging import ubkgLogging

class Jkgout:

    def __init__(self, ulog: ubkgLogging):

        # Obtain common application configuration.
        cfg = ubkgConfigParser(path='ubkgjkg.ini', ulog=ulog)
        self.jkg_edge = cfg.get_value(section='jkg_en', key='jkg_edge')
        self.jkg_node = cfg.get_value(section='jkg_en', key='jkg_node')

