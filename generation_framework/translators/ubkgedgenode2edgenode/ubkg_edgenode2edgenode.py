#!/usr/bin/env python
# coding: utf-8

"""
Copies a set of UBKG ingestion files in UBKG edges/nodes format from a specified local directory to the appropriate
directory in the local repo.

In some cases, local files are actually a GZIP of an OWL file. Unzip and then run PheKnowLator.
"""


import argparse
import sys
import os
import pandas as pd

# The following allows for an absolute import from an adjacent script directory--i.e., up and over instead of down.
# Find the absolute path. (This assumes that this script is being called from build_csv.py.)
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/ubkg_utilities')
sys.path.append(fpath)

# argparser
from classes.ubkg_args import RawTextArgumentDefaultsHelpFormatter

# Centralized logging module
from functions.find_repo_root import find_repo_root
from classes.ubkg_logging import ubkgLogging

# config file
from classes.ubkg_config import ubkgConfigParser

# Extraction module
from classes.ubkg_extract import ubkgExtract

def containsedgenodefiles(path: str) -> bool:
    # Checks files in a local path.

    # Files should be one of two types:
    # 1. Edges/nodes files
    # 2. An OWL file

    filelist = ['OWLNETS_node_metadata.txt', 'nodes.txt', 'nodes.tsv', 'OWLNETS_node_metadata.tsv',
                'OWLNETS_edgelist.txt', 'edges.txt', 'edges.tsv', 'OWLNETS_edgelist.tsv']

    for f in os.listdir(path):
        fpath = os.path.join(path, f)
        if os.path.isfile(fpath):
            # JULY 2025 exclude irrelevant files such as .DS_Store file.
            if f in filelist:
                dfTest = pd.read_csv(fpath, sep='\t', nrows=5)
                if 'subject' in dfTest.columns or 'node_id' in dfTest.columns: #or 'relation_id' in dfTest.columns:
                    return True

    return False

def getowlfilename(path: str) -> str:
    # Returns the first file in a path, under the assumption that the directory contains a single OWL file.
    for f in os.listdir(path):
        fpath = os.path.join(path, f)
        if os.path.isfile(fpath):
            return f

def getargs() -> argparse.Namespace:
    # Parse command line arguments.
    parser = argparse.ArgumentParser(
        description='Copies ingest files in UBKG edges/nodes format from a local directory.',
        formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument('sab', help='SAB for ingest files')
    parser.add_argument("-f", "--fetchnew", action="store_true", help="fetch new set of edge/node files ")

    args = parser.parse_args()

    return args

def unzipfiles(path: str, uext: ubkgExtract, ulog: ubkgLogging) -> None:
    """
    Decompresses all files in a folder path using Gzip.
    :param path: path to folder
    :param uext: ubkgExtract object
    :param ulog: ubkgLogging
    :return:
    """

    for f in os.listdir(path):
        fname = f.lower()
        fpath = os.path.join(path, fname)
        if 'gz' in fname:
            # Get file name before extension.
            funzip = os.path.join(path, fname.split('.gz')[0])
            # Decompress
            ulog.print_and_logger_info(f'Unzipping {fpath} to {funzip}')
            funzippath = uext.extract_from_gzip(zipfilename=fpath, outputpath=path, outfilename=funzip)

    return

def write_edges_file(source_path: str, edges_path: str, uext: ubkgExtract, sab:str) -> None:
    """
    Translates a UBKG edge file into a JKG edge file.
    :param source_path: path to source edge file
    :param edges_path: path to JKG edge file
    :param uext: extraction object
    :param sab: SAB
    :return:
    """

def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = ubkgLogging(log_dir=log_dir, log_file='ubkg_edgenode2edgenode.log')

    # Get runtime arguments.
    args = getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/translators/ubkg_edgenode2edgenode/ubkg_edgenode2edgenode.ini')
    config = ubkgConfigParser(path=cfgpath, ulog=ulog)

    # Get the sab_jkg directory to which to copy translated files.
    # The config file contains absolute paths to the parent directories in the local repo.
    # Affix the SAB to the paths.
    sab_jkg_dir = os.path.join(os.path.dirname(os.getcwd()), config.get_value(section='Directories', key='sab_jkg_dir'))
    sab_jkg_dir_sab = os.path.join(sab_jkg_dir, args.sab)

    sab_source_dir = os.path.join(os.path.dirname(os.getcwd()), config.get_value(section='Directories', key='sab_source_dir'))
    sab_source_dir_sab = os.path.join(sab_source_dir, args.sab)
    ulog.print_and_logger_info(f'Making directories {sab_source_dir} and {sab_jkg_dir}')
    # Make the subdirectories.
    os.system(f'mkdir -p {sab_jkg_dir_sab}')
    os.system(f'mkdir -p {sab_source_dir_sab}')

    # Get the appropriate file path.
    frompath = config.get_value(section='Paths', key=args.sab)
    ulog.print_and_logger_info(f'Copying {frompath} to {sab_jkg_dir_sab}')

    # Decompress files if necessary.
    uext = ubkgExtract(ulog=ulog)
    unzipfiles(path=frompath, uext=uext, ulog=ulog)

    if containsedgenodefiles(frompath):

        # Copy files from the local path to the owlnets path.
        ulog.print_and_logger_info(f'Files in {frompath} are in edges/nodes format: copying to {sab_source_dir_sab}')
        os.system(f'cp {frompath}/*.* {sab_source_dir_sab}')

        # Translate to JKG OWLNETS format.
        write_edges_file(source_path=sab_source_dir_sab, edges_path=sab_jkg_dir_sab, uext=uext, sab=args.sab)

        print('TRANSLATE EDGE FILE')
        print('TRANSLATE NODE FILE')

    else:

        ulog.print_and_logger_error(f'Files in {frompath} are not in edges/nodes format.')
        exit(1)

if __name__ == "__main__":
    main()