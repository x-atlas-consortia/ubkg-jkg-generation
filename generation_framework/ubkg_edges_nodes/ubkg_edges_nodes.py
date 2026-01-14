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
from ubkg_args import RawTextArgumentDefaultsHelpFormatter

# Centralized logging module
from find_repo_root import find_repo_root
from ubkg_logging import UbkgLogging

# config file
from ubkg_config import ubkgConfigParser

# Calling subprocesses
import ubkg_subprocess as usub
# Extracting files
import ubkg_extract as uextract

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

def unzipfiles(path: str, ulog: UbkgLogging) -> None:
    """
    Decompresses all files in a folder path using Gzip.
    :param path: path to folder
    :param ulog: UbkgLogging
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
            funzippath = uextract.extract_from_gzip(zipfilename=fpath, outputpath=path, outfilename=funzip)

    return

def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = UbkgLogging(log_dir=log_dir, log_file='ubkg.log')

    # Get runtime arguments.
    args = getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/ubkg_edges_nodes/edges_nodes.ini')
    config = ubkgConfigParser(path=cfgpath, log_dir=log_dir, log_file='ubkg.log')

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
    unzipfiles(path=frompath, ulog=ulog)

    if containsedgenodefiles(frompath):

        # Copy files from the local path to the owlnets path.
        ulog.print_and_logger_info(f'Files in {frompath} are in edges/nodes format: copying to {sab_jkg_dir_sab}')
        os.system(f'cp {frompath}/*.* {sab_jkg_dir_sab}')

    else:

        # Assume the folder contains a single OWL file that should be converted to OWLNETS format.
        ulog.print_and_logger_info(f'Files in {frompath} are not in edges/nodes format.')
        ulog.print_and_logger_info('Script will assume that the file is an OWL and will run PheKnowLator script.')
        os.system(f'cp {frompath}/*.* {sab_source_dir_sab}')
        url = os.path.join(sab_source_dir_sab, getowlfilename(frompath))

        pheknowlator_script_py = os.path.join(find_repo_root(), config.get_value(section='PheKnowLator', key='pheknowlator_script_py'))
        owltools_dir = os.path.join(repo_root,config.get_value(section='PheKnowLator',key='owltools_dir'))

        # Call OWLNETS script using the path to the local copy of the OWL file as the url.
        owlnets_script: str = (f"{pheknowlator_script_py} "
                               f"--ignore_owl_md5 "
                               f"--clean "
                               f"--verbose "
                               f"--with_imports "
                               f"--owlnets_dir {sab_jkg_dir} "
                               f"--owltools_dir {owltools_dir} "
                               f"--owl_dir {sab_source_dir} "
                               f" {url} {args.sab}")
        ulog.print_and_logger_info(f"Running: {owlnets_script}")
        usub.call_subprocess(owlnets_script)

if __name__ == "__main__":
    main()