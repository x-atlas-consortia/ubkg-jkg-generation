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
# sources.json handling
from utilities.classes.ubkg_sources import ubkgSources
# JKG JSON handling
from utilities.classes.jkg_json import Jkgjson
# JKG edge and node file handling
from utilities.classes.jkg_edgenode import Jkgedgenode

# Manager of import of a SAB's JKGEN data to the JKGJSON
from utilities.classes.sab_jkg_import import Sabjkgimport

# Writing to JKG JSON
#from utilities.classes.json_writer import JsonWriter

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

def get_sab_source_node(sab:str, ulog:ubkgLogging, cfg:ubkgConfigParser, repo_root:str)->dict:
    """
    Builds a JKG JSON Source node for a non-UMLS SAB.
    :param sab: SAB
    :param ulog: logging object
    :param cfg: configuration object
    :param repo_root: repository root
    """
    usource = ubkgSources(ulog=ulog, cfg=cfg, repo_root=repo_root)
    source_type = usource.get(sab=sab, key='source_type')
    source_name = usource.get(sab=sab, key='name')
    source_description = usource.get(sab=sab, key='description')
    source_version = usource.get(sab=sab, key='version')
    dictsource = {
        "labels": ["Source"],
        "properties":
            {"id": f"{sab.upper()}:{sab.upper()}",
             "name": source_name,
             "description": source_description,
             "sab": f"{sab.upper()}",
             "source_version": source_version}
    }
    if source_type == "owl":
        dictsource["properties"]["source"] = usource.get(sab=sab, key='owl_url')

    return dictsource

def main():

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

        # Initialize the JKG Import manager for the SAB.
        # The import manager will build nodes and rels arrays to
        # add to the JKG JSON.
        jkg_import = Sabjkgimport(sab=sab_name, ulog=ulog, cfg=cfg,  repo_root=repo_root)

        # Build the source node for the SAB.
        list_source_nodes = [get_sab_source_node(sab=sab_name,
                                                 ulog=ulog, cfg=cfg,
                                                 repo_root=repo_root)]

        # Add the source node to the new nodes array.
        jkg_import.new_jkg_json_nodes = (jkg_import.new_jkg_json_nodes
                                         + list_source_nodes)


if __name__ == "__main__":
    main()