#!/usr/bin/env python
"""
cedar_entity

2026

UBKG ETL that maps templates from CEDAR to HuBMAP/SenNet provenance entities.

Assumes that the following SABs have already been ingested into the ontology CSVs.
CEDAR
HUBMAP
SENNET

"""

import argparse
import sys
import os

import numpy as np
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
from ubkg_logging import ubkgLogging

# config file
from ubkg_config import ubkgConfigParser

def initialize_file(path: str, ulog:ubkgLogging, file_type: str):
    """
    Creates and writes header for edge and node file.

    :param path: path to edge file
    :param file_type: edge or node
    :return:
    """

    ulog.print_and_logger_info('Building: ' + os.path.abspath(path))

    if file_type == 'edge':
        header = 'subject\tpredicate\tobject\n'
    else:
        header = 'node_id\tnode_namespace\tnode_label\tnode_definition\tnode_synonyms\tnode_dbxrefs\n'

    with open(path, 'w') as out:
        out.write(header)

    return

def getargs()->argparse.Namespace:
    # Parse command line arguments.
    parser = argparse.ArgumentParser(description='Builds ontology files in OWLNETS format from CEDAR entities.',
    formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument("sab", help="SAB")
    parser.add_argument("-f", "--fetchnew", action="store_true", help="fetch new set of edge/node files ")
    args = parser.parse_args()

    return args

def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = ubkgLogging(log_dir=log_dir, log_file='ubkg.log')

    # Obtain runtime arguments.
    args = getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/cedar_entity/cedar_entity.ini')
    cfg = ubkgConfigParser(path=cfgpath, log_dir=log_dir, log_file='ubkg.log')

    # Get sab_source and sab_jkg directories.
    # The config file contains absolute paths to the parent directories in the local repo.
    # Affix the SAB to the paths.
    sab_source_dir = os.path.join(os.path.dirname(os.getcwd()),
                                  cfg.get_value(section='Directories', key='sab_source_dir'), args.sab)
    sab_jkg_dir = os.path.join(os.path.dirname(os.getcwd()), cfg.get_value(section='Directories', key='sab_jkg_dir'),
                               args.sab)


    # Make the output directory.
    os.system(f'mkdir -p {sab_jkg_dir}')

    # Get the appropriate file path. Assume that CEDAR has been ingested.
    frompath = cfg.get_value(section='Paths', key='CEDAR')

    # Read the CEDAR edge file.
    dfcedaredge = pd.read_csv(frompath, delimiter='\t')
    # Filter the CEDAR edge file to those nodes that are children of the template parent.
    dftemplate = dfcedaredge[dfcedaredge['object'] == 'https://schema.metadatacenter.org/core/Template']

    # Build list of CEDAR template node ids.
    listcedarids = []
    for index, row in dftemplate.iterrows():
        idsplit = row['subject'].split('/')
        listcedarids.append(f'CEDAR:{idsplit[len(idsplit)-1]}')


    # BUILD THE NODE FILE.
    # Initialize the node file.
    nodes_path: str = os.path.join(sab_jkg_dir, 'OWLNETS_node_metadata.txt')
    ulog.print_and_logger_info(f'Writing nodes file at {nodes_path}...')
    initialize_file(path=nodes_path, ulog=ulog, file_type='node')

    # Write CEDAR template node ids to the node file. This assumes that CEDAR has already been ingested.
    with open(nodes_path, 'a') as out:
        for lid in listcedarids:
            node_id = lid
            out.write(f'{node_id}\n')

    # BUILD THE EDGE FILE.
    # Initialize the edge file.
    edgelist_path: str = os.path.join(sab_jkg_dir, 'OWLNETS_edgelist.txt')
    ulog.print_and_logger_info(f'Writing edge file to {edgelist_path}...')
    initialize_file(path=edgelist_path, ulog=ulog, file_type='edge')

    # Read the map of templates that do not map to dataset.
    map_path: str = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/cedar_entity/cedar_entity.tsv')
    dfmap = pd.read_csv(map_path, delimiter='\t')

    # Write 'used_in_entity' relationships between each CEDAR template node and the appropriate provenance entities in
    # HuBMAP and SenNet.

    with open(edgelist_path, 'a') as out:
        for id in listcedarids:
            subject = id
            predicate = 'used_in_entity'
            dftest = dfmap[dfmap['id'] == id]
            if dftest.empty:
                # dataset entities
                # HUBMAP:C040001
                # SENNET:C050002
                obj = 'HUBMAP:C040001'
                out.write(f'{subject}\t{predicate}\t{obj}\n')
                obj = 'SENNET:C050002'
                out.write(f'{subject}\t{predicate}\t{obj}\n')
            else:
                obj = dftest.iloc[0]['hubmap']
                if obj is not np.nan:
                    out.write(f'{subject}\t{predicate}\t{obj}\n')
                obj = dftest.iloc[0]['sennet']
                if obj is not np.nan:
                    out.write(f'{subject}\t{predicate}\t{obj}\n')

if __name__ == "__main__":
    main()