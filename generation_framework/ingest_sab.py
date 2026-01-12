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

from ubkg_utilities.find_repo_root import find_repo_root

# Logging module
from ubkg_utilities.ubkg_logging import UbkgLogging
# argparser
from ubkg_utilities.ubkg_args import RawTextArgumentDefaultsHelpFormatter

# config file
from ubkg_utilities.ubkg_config import ubkgConfigParser

# Subprocess handling
#import ubkg_utilities.ubkg_subprocess as usub

#----------------------------------

def getargs() -> argparse.Namespace:
    """
    Obtains command line arguments.
    :return: parsed command line arguments
    """
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

    return args

def main():

    # Set up logging.
    ulog = UbkgLogging()
    ulog.print_and_logger_info('-' * 50)

    # Obtain command line arguments.
    args = getargs()
    ulog.print_and_logger_info('Command line arguments:')
    sab_names = [s.upper() for s in args.sabs]
    ulog.print_and_logger_info(f' - SABs: {', '.join(sab_names)}')
    ulog.print_and_logger_info(f' - fetch fresh copies: {args.fetch}')

    # Obtain configuration information.
    ulog.print_and_logger_info('Getting configuration...')
    cfg = ubkgConfigParser('ingest_sab.ini')
    repo_root = find_repo_root()

    # Get absolute directory paths.
    sab_json = os.path.join(repo_root, cfg.get_value(section='sabs',key='sab_json'))
    sab_source_dir = os.path.join(repo_root,cfg.get_value(section='directories',key='sab_source_dir'))
    sab_jkg_dir = os.path.join(repo_root,cfg.get_value(section='directories',key='sab_jkg_dir'))
    owltools_dir = os.path.join(repo_root,cfg.get_value(section='directories',key='owltools_dir'))
    ulog.print_and_logger_info(f'sab_json: {sab_json}')
    ulog.print_and_logger_info(f'sab_source_dir: {sab_source_dir}')
    ulog.print_and_logger_info(f'sab_jkg_dir: {sab_jkg_dir}')
    ulog.print_and_logger_info(f'owltools_dir: {owltools_dir}')



if __name__ == "__main__":
    main()