"""
edgenode2jkg.py
Script that "ingests" non-UMLS data--i.e., adds data from a non-UMLS
data source into a JKG JSON.

Tasks:
1. Reads a "JKG JSON"--a JSON file that conforms to the JKG Specification
2. Reads a set of "edge/node files"--files in JKG Edge/Node (JKGEN)
   format generated from a translation of data from a non-UMLS data
   source--e.g., an OWL file
3. Adds to the nodes and rels arrays in the JKG JSON objects with data
   obtained from the JKGEN files.

A critical part of ingestion is identifying "equivalence classes", or
cross-references between concepts and codes.
"""
import os
import time
from datetime import timedelta
import argparse
import tqdm

# argparser
from utilities.classes.ubkg_args import RawTextArgumentDefaultsHelpFormatter

# Logging module
from utilities.classes.ubkg_logging import ubkgLogging
# Config file
from utilities.classes.ubkg_config import ubkgConfigParser
# Manager of import of a SAB's JKGEN data to the JKGJSON
from utilities.classes.sab_jkg_import import Sabjkgimport
# Subprocess handling
from utilities.functions.find_repo_root import find_repo_root
from utilities.classes.ubkg_sources import ubkgSources

def getargs() -> argparse.Namespace:
    """
    Obtains command line arguments.
    :return: parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Ingest non-UMLS data into a JKG JSON file.',
        formatter_class=RawTextArgumentDefaultsHelpFormatter)

    # Multiple SABs can be ingested as a space-delimited list.
    parser.add_argument('sabs', nargs='*', help='space-delimited list of SABs')

    args = parser.parse_args()

    return args

def main():

    start_time = time.time()

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = ubkgLogging(log_dir=log_dir, log_file='jkgen2jkg.log')
    ulog.print_and_logger_info('-' * 50)

    args = getargs()
    ulog.print_and_logger_info('Command line arguments:')
    sab_names = [s.upper() for s in args.sabs]
    ulog.print_and_logger_info(f' - SABs: {', '.join(sab_names)}')

    # Obtain application configuration.
    cfg = ubkgConfigParser(path='ubkgjkg.ini', ulog=ulog)

    # Read and validate the file of SAB-specific configuration.
    usource = ubkgSources(ulog=ulog, cfg=cfg, repo_root=repo_root)
    sab_names = [s.upper() for s in args.sabs]

    # For each SAB,
    for sab_name in sab_names:

        """
        Initialize the JKG Import manager for the SAB.
        The import manager will :
        1. Build lists of nodes and rels from the SAB's JKGEN files.
        2. Add nodes and rels to the JKG JSON file.
        3. Write a new JKG JSON file.
        """

        jkg_import = Sabjkgimport(sab=sab_name, ulog=ulog, cfg=cfg,  repo_root=repo_root)

    elapsed_time = time.time() - start_time
    ulog.print_and_logger_info(f'Completed. Total Elapsed time {"{:0>8}".format(str(timedelta(seconds=elapsed_time)))}')


if __name__ == "__main__":
    main()