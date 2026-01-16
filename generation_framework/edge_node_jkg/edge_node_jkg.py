#!/usr/bin/env python
# coding: utf-8

"""
Converts UBKG edge and node files to JKG JSON format.
"""

import os
import sys

import argparse
from tqdm import tqdm
# Import UBKG utilities which is in a directory that is at the same level as the script directory.
# Go "up and over" for an absolute path.
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/ubkg_utilities')
sys.path.append(fpath)

# argparser
from ubkg_args import RawTextArgumentDefaultsHelpFormatter
# Centralized logging module
from find_repo_root import find_repo_root
from ubkg_logging import UbkgLogging

# config file
from ubkg_config import ubkgConfigParser


def getargs() -> argparse.Namespace:

    # Parse arguments.
    parser = argparse.ArgumentParser(
    description='Convert UBKG edge and node files to JKG JSON format',
    formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument("sab", help="Identifier for cell type annotation")
    args = parser.parse_args()

    return args

def main():
    getargs()

if __name__ == "__main__":
    main()