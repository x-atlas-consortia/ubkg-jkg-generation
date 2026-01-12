#!/usr/bin/env python
"""
2026
ingest_sab.py

ingest_sab.py is the foundation of the UBKG-JKG generation framework.

The framework is a set of ETL scripts that obtain information from multiple
source formats, including
- OWL files
- spreadsheets
- downloads from websites and APIs

ETLs are configured via the sab.json file.
"""

import os
import shutil
import time
import re
import subprocess
from datetime import timedelta
import json
from typing import List
import sys
import argparse

# Logging module
from ubkg_utilities.ubkg_logging import UbkgLogging
# argparser
from ubkg_utilities.ubkg_args import RawTextArgumentDefaultsHelpFormatter

# Subprocess handling
#import ubkg_utilities.ubkg_subprocess as usub
# config file
#import ubkg_utilities.ubkg_config as uconfig

def main():

    # Set up logging.
    ulog = UbkgLogging()
    ulog.print_and_logger_info('Starting ingest_sab.py')

    # Obtain runtime arguments.
    parser = argparse.ArgumentParser(
        description='Extract source files and transform to JKG format',
        formatter_class=RawTextArgumentDefaultsHelpFormatter)

    # Multiple SABs can be ingested as a space-delimited list.
    parser.add_argument('sabs', nargs='*', help='space-delimited list of SABs')

    # If -f specified, obtain a fresh copy of the source files; otherwise,
    # use the stored version.
    parser.add_argument("-f", "--fetch", action="store_true",
                             help='fetch fresh copy of source files')

    args = parser.parse_args()

    ulog.print_and_logger_info('-' * 50)
    ulog.print_and_logger_info('Runtime arguments:')
    sab_names = [s.upper() for s in args.sabs]
    ulog.print_and_logger_info(f' - SABs: {', '.join(sab_names)}')
    ulog.print_and_logger_info(f' - fetch fresh copies: {args.fetch}')

if __name__ == "__main__":
    main()