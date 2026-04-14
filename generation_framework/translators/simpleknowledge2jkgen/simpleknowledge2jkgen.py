#!/usr/bin/env python
# coding: utf-8

"""
SimpleKnowledge to OWLNETS converter

Uses the input spreadsheet for the SimpleKnowledge Editor to generate a set of text files that comply with the
OWLNETS format, as described in https://github.com/callahantiff/PheKnowLator/blob/master/notebooks
/OWLNETS_Example_Application.ipynb.

User guide to build the SimpleKnowledge Editor spreadsheet:
https://docs.google.com/document/d/1wjsOzJYRV2FRehX7NQI74ZKRXvH45l0QmClBBF_VypM/edit?usp=sharing

The OWLNETS format represents ontology data in a TSV in format:

subject <tab> predicate <tab> object

where:
   subject - code for node in custom ontology
   predicate - relationship
   object: another code in the custom ontology

(In canonical OWLNETS, codes and relationships are IRIs from standard
OBO ontology, such as RO.)
For custom ontologies such as HuBMAP, we use custom relationship strings.)
"""

# ----------------------------
import argparse
import sys
import pandas as pd
import numpy as np
import os

# This script uses the codeReplacements function, which is currently in the module
# generation_framework/utilities/parsetools.py

# The following allows for an absolute import from an adjacent script directory--i.e., up and over instead of down.
# Find the absolute path. (This assumes that this script is being called from build_csv.py.)
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/utilities')
sys.path.append(fpath)

# argparser
from classes.ubkg_args import RawTextArgumentDefaultsHelpFormatter

from classes.ubkg_standardizer import ubkgStandardizer
# Extraction module
from classes.ubkg_extract import ubkgExtract

# Centralized logging module
from functions.find_repo_root import find_repo_root
from classes.ubkg_logging import ubkgLogging

# config file
from classes.ubkg_config import ubkgConfigParser

def download_source_file(cfg: ubkgConfigParser, ulog: ubkgLogging, uext: ubkgExtract, sab: str, sab_source_dir: str, sab_jkg_dir: str) -> str:

    """
    Obtains SimpleKnowledge source spreadsheet from either
    - Google Drive
    - GitHub repository
    :param cfg: application configuration
    :param ulog: logging object
    :param uext: UbkgExtract object
    :param sab: SAB
    :param sab_source_dir: SAB source directory
    :param sab_jkg_dir: SAB JKG directory
    :return:
    """

    ulog.print_and_logger_info(f'Making directories {sab_source_dir} and {sab_jkg_dir}...')
    # Create output folders for source files. Use the existing /sab_source and /sab_jkg folder structure.
    os.system(f'mkdir -p {sab_source_dir}')
    os.system(f'mkdir -p {sab_jkg_dir}')

    # Get the URL to the spreadsheet.
    url = cfg.get_value(section='URL',key=sab)
    filepath = os.path.join(sab_source_dir,'SimpleKnowledge.xlsx')
    ulog.print_and_logger_info(f'Downloading {url}...')
    if 'google' in url.lower():
        # Download Google sheet.
        uext.download_file_from_google_drive(share_url=url, download_full_path=filepath)
    else:
        # Download spreadsheet from GitHub repo.
        uext.download_file_from_github(share_url=url, download_full_path=filepath)

    return filepath

def write_edges_file(dfsk: pd.DataFrame, out_dir: str, ulog: ubkgLogging, ustand: ubkgStandardizer, sab:str):

    """
    Writes an edges file in OWLNETS format.
    :param dfsk: DataFrame from a SimpleKnowledge spreadsheet
    :param out_dir: output directory
    :param ulog: logging object
    :param ustand: UbkgStandardizer object
    :param sab: SAB
    :return:
    """

    edgelist_path: str = os.path.join(out_dir, 'OWLNETS_edgelist.txt')
    ulog.print_and_logger_info('Building: ' + os.path.abspath(edgelist_path))

    # List of edge rows.
    rows = []

    """
        Each column after E in the spreadsheet (isa, etc.) represents a type of
        subject-predicate_object relationship.
        1. Column E represents the dbxrefs. The dbxrefs field is an optional list of references to
            concept IDs in other vocabularies, delimited
            with a comma between SAB and ID and a pipe between entries--e.g,
            SNOMEDCT_US:999999,UMLS:C99999. This list should be exploded and then subClassOf relationship rows
            written.
        2. Column F represents the isa relationship. This corresponds to an isa relationship within the
           custom ontology.
        3. Columns after F represent relationships other than isa in the custom ontology.

           Cells in relationship columns contain comma-separated lists of object nodes.

           Thus, a relationship cell, in general, represents a set of subject-predicate-object
           relationships between the concept in the "code" cell and the concepts in the relationship
           cell.
        """

    for index, row in dfsk.iterrows():
        subject = str(row['code'])

        for col in range(5, len(row)):
            if col == 5:
                predicate_uri = "isa"
            else:
                predicate_uri = dfsk.columns[col]

            objects = row.iloc[col]

            if not pd.isna(objects):
                listobjects = objects.split(',')
                for obj in listobjects:
                    if col == 4:
                        objcode = obj
                    else:
                        match = dfsk[dfsk['term'] == obj]
                        if match.size == 0:
                            err = (f"Error: row for '{subject}' indicates relationship '{predicate_uri}'"
                                   f" with node '{obj}', but this node is not defined in the 'term'"
                                   f" column. (Check for spelling and case of node name.)")
                            ulog.print_and_logger_error(err)
                            exit(1)
                        objcode = match.iloc[0, 1]

                    rows.append({'subject': subject, 'predicate': predicate_uri, 'object': str(objcode)})

    # Standardize codes and edges and write to file.
    dfedges: pd.DataFrame = pd.DataFrame(rows, columns=['subject', 'predicate', 'object'])
    dfedges['subject'] = ustand.standardize_code(dfedges['subject'], sab=sab)
    dfedges['object'] = ustand.standardize_code(dfedges['object'], sab=sab)
    dfedges['predicate'] = ustand.standardize_relationships(dfedges['predicate'])

    # Write to file
    dfedges.to_csv(edgelist_path, sep='\t', index=False, header=False)

def write_nodes_file(dfsk: pd.DataFrame, out_dir: str, ulog: ubkgLogging):

    """
    Writes a nodes file in OWLNETS format.
    :param dfsk: DataFrame from a SimpleKnowledge spreadsheet
    :param out_dir: output directory
    :param ulog: logging object
    :return:
    """

    # NODE METADATA
    # Write a row for each unique concept in the 'code' column.

    node_metadata_path: str = os.path.join(out_dir, 'OWLNETS_node_metadata.txt')
    ulog.print_and_logger_info('Building: ' + os.path.abspath(node_metadata_path))

    # Work on a copy to avoid modifying the original
    dfout: pd.DataFrame = dfsk.copy()

    # Map columns to output names
    dfout = dfout.rename(columns={
        'code': 'node_id',
        'term': 'node_label',
        'definition': 'node_definition',
        'synonyms': 'node_synonyms',
        'dbxrefs': 'node_dbxrefs'
    })

    # Add empty namespace column
    #dfout['node_namespace'] = ''

    # Strip '_vitessce_hint' suffix from node_label
    dfout['node_label'] = dfout['node_label'].where(
        ~dfout['node_label'].str.contains('_vitessce_hint', na=False),
        dfout['node_label'].str.split('_vitessce_hint').str[0]
    )

    # Replace nan values with empty string for optional fields
    dfout['node_synonyms'] = dfout['node_synonyms'].replace('nan', '').fillna('')
    dfout['node_dbxrefs'] = dfout['node_dbxrefs'].replace('nan', '').fillna('')

    # Write to file
    dfout[['node_id', 'node_label', 'node_definition', 'node_synonyms', 'node_dbxrefs']].to_csv(
        node_metadata_path, sep='\t', index=False
    )

def getargs()->argparse.Namespace:

    # Parse command line arguments.
    parser = argparse.ArgumentParser(description='Builds ontology files in OWLNETS format from a spreadsheet in SimpleKnowledge format.',
    formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument("sab", help="SAB for the SimpleKnowledge ontology")
    parser.add_argument("-f", "--fetchnew", action="store_true", help="fetch new Simpleknowledge spreadsheet ")
    args = parser.parse_args()

    return args

def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    # The logging directory location is determined by the pkt-kg package.
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = ubkgLogging(log_dir=log_dir, log_file='simpleknowledge2jkgen.log')

    ustand = ubkgStandardizer(ulog=ulog, repo_root=repo_root)

    # Get runtime arguments.
    args = getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/translators/simpleknowledge2jkgen/simpleknowledge2jkgen.ini')
    skowlnets_config = ubkgConfigParser(path=cfgpath, ulog=ulog)

    # Get sab and sab_jkg directories.
    # The config file contains absolute paths to the parent directories in the local repo.
    # Affix the SAB to the paths.
    sab_source_dir = os.path.join(os.path.dirname(os.getcwd()),skowlnets_config.get_value(section='Directories',key='sab_source_dir'),args.sab)
    sab_jkg_dir = os.path.join(os.path.dirname(os.getcwd()),skowlnets_config.get_value(section='Directories',key='sab_jkg_dir'),args.sab)

    uext = ubkgExtract(ulog=ulog)

    if args.fetchnew:
        # Download the SimpleKnowledge spreadsheet.
        sk_file = download_source_file(cfg=skowlnets_config, ulog=ulog, uext=uext, sab=args.sab, sab_source_dir=sab_source_dir, sab_jkg_dir=sab_jkg_dir)
    else:
        # Use the existing SimpleKnowledge spreadsheet.
        ulog.print_and_logger_info('Using existing SimpleKnowledge spreadsheet.')
        sk_file = os.path.join(sab_source_dir,'SimpleKnowledge.xlsx')

    # Load SimpleKnowledge spreadsheet into a DataFrame.
    df_simpleknowledge = pd.read_excel(sk_file)

    # Generate the OWLNETS files.
    write_edges_file(dfsk=df_simpleknowledge,out_dir=sab_jkg_dir, ulog=ulog, ustand=ustand, sab=args.sab)
    write_nodes_file(dfsk=df_simpleknowledge,out_dir=sab_jkg_dir, ulog=ulog)

if __name__ == "__main__":
    main()