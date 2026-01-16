#!/usr/bin/env python
# coding: utf-8

# Converts the TSV file downloaded (as a GZ archive) from UniProt for the UniProtKB
# ontology to OWLNETS format.

# This script is designed to align with the conversion logic executed in the build_csv.py script--e.g., outputs to
# owlnets_output, etc. This means: 1. The UNIPROTKB CSV file will be extracted from GZ and downloaded to the OWL folder
# path, even though it is not an OWL file. 2. The OWLNETS output will be stored in the OWLNETS folder path.

# January 2025 - enhanced to include UniProtKB GO annotations.

import sys
import pandas as pd
import numpy as np
import os
import requests
import argparse
import re
from tqdm import tqdm

# Import UBKG utilities that are in a directory that is at the same level as the script directory.
# Go "up and over" for an absolute path.
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/ubkg_utilities')
sys.path.append(fpath)
# Extraction module
import ubkg_extract as uextract
# argparser
from ubkg_args import RawTextArgumentDefaultsHelpFormatter
# Centralized logging module
from find_repo_root import find_repo_root
from ubkg_logging import UbkgLogging

# config file
from ubkg_config import ubkgConfigParser

import ubkg_parsetools as uparse
# -----------------------------

def getuniprotkb(cfg: ubkgConfigParser, ulog: UbkgLogging, sab_source_dir: str, sab_jkg_dir: str) -> pd.DataFrame:

    """
    Executes queries in the UNIPROTKB API that downloads GZip files of UNIPROTKB data.
    Extracts the contents of the GZips--which should be TSVs.
    Loads the contents into a DataFrame.
    Consolidates DataFrames into a single TSV if more than one organism was specified in the query.

    :param cfg: an instance of the ubkgConfigParser class, which works with the application configuration file.
    :param ulog: UbkgLogging instance
    :param sab_source_dir: directory to which to download the GZIP file.
    :param sab_jkg_dir: directory to which to extract the contents of the GZIP--a TSV file.
    :return:

    Results:
    1. A file named UNIPROTKB_x.tsv for every organism x identified in the configuration file.
    2. A file named UNIPROTKB_ALL.tsv that consolidates all organism data. This file will be created even if there
       is only one organism specified in the configuration file.
    """

    # Query URL
    base_url = cfg.get_value(section='URL', key='BaseQuery')

    # Determine whether to filter to only reviewed (SwissProt).
    reviewed = cfg.get_value(section='Filters', key='reviewed')
    rev = ''
    msgreviewed = ' - both manually curated (SwissProt) and automatically curated (TrEMBL)'
    if reviewed == 'True':
        rev = 'AND%20%28reviewed%3Atrue%29'
        msgreviewed = ' - manually curated (SwissProt) only'

    # Query for each organism in the list.
    list_org = []
    for org in cfg.config['Organisms']:
        # Determine the organism for which to download UNIPROTKB data.
        organism = cfg.get_value(section='Organisms', key=org)
        ulog.print_and_logger_info(f'Downloading for organism: {org} {msgreviewed}')

        # Download GZip file and extract the TSV file from it.
        zipfilename = f'UNIPROTKB_{org}.gz'
        tsvfilename = f'UNIPROTKB_{org}.tsv'

        url = base_url+organism+rev
        uextract.get_gzipped_file(gzip_url=url, zip_path=sab_source_dir, extract_path=sab_jkg_dir, zipfilename=zipfilename,
                                  outfilename=tsvfilename)

        # Load the extracted TSV file into a DataFrame.
        tsvpath = os.path.join(sab_jkg_dir, tsvfilename)
        list_org.append(uextract.read_csv_with_progress_bar(path=tsvpath, sep='\t', on_bad_lines='skip'))

        # Select only manually curated (SwissProt) proteins.
        # df = df[df['Reviewed'] == 'reviewed'].dropna(subset=['Gene Names']).reset_index(drop=True)

    # Concatenate DataFrames for all organisms.
    df = pd.concat(list_org, axis=0, ignore_index=True)
    df = df.fillna('')

    ulog.print_and_logger_info('Writing consolidated TSV UNIPROTKB_all.tsv')
    path_consolidated = os.path.join(sab_jkg_dir, 'UNIPROTKB_all.tsv')
    uextract.to_csv_with_progress_bar(df=df, path=path_consolidated, sep='\t')
    return df


def getallhgncid(cfg: ubkgConfigParser, ulog:UbkgLogging, sab_source_dir: str) -> pd.DataFrame:
    """
    Downloads all HGNC IDs from genenames.org.
    :param cfg:  an instance of the ubkgConfigParser class, which works with the application configuration file.
    :param ulog: UbkgLogging instance
    :param sab_source_dir: directory to which to download the GZIP file.
    :return:
    """

    ulog.print_and_logger_info('Downloading HGNC ID file from genenames.org')
    # Obtain the URL from the configuration file.
    url = cfg.get_value(section='HGNC', key='URL')

    hgnc_path = os.path.join(sab_source_dir, 'HGNC.TSV')
    uextract.download_file(url=url, download_full_path=hgnc_path)

    return uextract.read_csv_with_progress_bar(path=hgnc_path, sep='\t')


def gethgncid(hgnc_acronym: str):

    # Queries the HGNC REST API to obtain the HGNC ID, given an acronym.
    # This is slow: the getallhgncid function is the preferred method.

    hgnc = ''
    urlhgnc = 'http://rest.genenames.org/search/symbol/' + hgnc_acronym
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    response = requests.get(urlhgnc, headers=headers)
    if response.status_code == 200:
        numfound = response.json().get('response').get('numFound')
        if numfound >= 0:
            docs = response.json().get('response').get('docs')
            if len(docs) > 0:
                hgnc = docs[0].get('hgnc_id')
    return hgnc

def get_go_ids(go_column: str) -> list[str]:
    """
    Parse GO codes from a GO column string.

    The GO annotations in a GO column of the UniProtKB stream output are in a semicolon-delimited list
    in a fixed format--e.g.,
    amyloid-beta metabolic process [GO:0050435]; apoptotic process [GO:0006915]

    :param go_column: column content

    """
    list_go = go_column.split(';')
    list_return = []
    for go in list_go:
        if '[' in go:
            goid = go.split('[')[1]
            goid = goid.split(']')[0]
            list_return.append(goid)

    return list_return

def write_go_annotation_edges(subject: str, go_column: str, go_aspect: str, out):
    """
    Writes assertions between a protein and GO.
    The three GO columns in the UniProtKB stream download correspond to the three Gene Ontology Annotation (GOA)
    ontologies.
    Use as edge predicates the default relationships for each aspect.
    (The GOA itself has a higher resolution for the Cellular Component aspect, but UniProtKB groups annotations only
    at the level of aspect.)
    Aspect code   Description         default relation    Relations Ontology
    p             Biological Process  involved_in         http://purl.obolibrary.org/obo/RO_0002331
    c             Cellular Component  part_of             http://purl.obolibrary.org/obo/BFO_0000050
    f             Molecular Function  enables             http://purl.obolibrary.org/obo/RO_0002327

    The GO annotations in each column are in a semicolon-delimited list in a fixed format--e.g.,
    amyloid-beta metabolic process [GO:0050435]; apoptotic process [GO:0006915]

    :param subject: UniProtKB ID for the assertion of the GO annotation
    :param go_column: semicolon-delimited list of GO annotations
    :param go_aspect: one-letter code for the GOA aspect
    :param out: output file pointer
    """

    if go_aspect == 'p':
        pred = 'http://purl.obolibrary.org/obo/RO_0002331'
    elif go_aspect == 'c':
        pred = 'http://purl.obolibrary.org/obo/BFO_0000050'
    elif go_aspect == 'f':
        pred = 'http://purl.obolibrary.org/obo/RO_0002327'
    else:
        raise ValueError(f"Invalid GO aspect parameter '{go_aspect}'.")

    list_goid = get_go_ids(go_column=go_column)
    for goid in list_goid:
        out.write(subject + '\t' + pred + '\t' + goid + '\n')


def write_edges_file(df: pd.DataFrame, ulog: UbkgLogging, dfhgnc:pd.DataFrame, sab_jkg_dir: str):

    """
    Writes an edges file in OWLNETS format.
    :param df: DataFrame of UNIPROTKB data
    :param dfhgnc: DataFrame of HGNC data
    :param sab_jkg_dir: output directory
    :return:
    """

    # The OWLNETS format represents ontology data in a TSV in format:

    # subject <tab> predicate <tab> object
    #
    # where:
    #   subject - code for node in custom ontology
    #   predicate - relationship
    #   object: another code in the custom ontology

    #  (In canonical OWLNETS, the relationship is a URI for a relation
    #  property in a standard OBO ontology, such as RO.) For custom
    #  ontologies such as HuBMAP, we use custom relationship strings.)

    edgelist_path: str = os.path.join(sab_jkg_dir, 'OWLNETS_edgelist.txt')

    ulog.print_and_logger_info('Building: ' + os.path.abspath(edgelist_path))

    pred = 'http://purl.obolibrary.org/obo/RO_0002204'  # gene product of

    with open(edgelist_path, 'w') as out:
        out.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
        # Show TQDM progress bar.
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            subject = 'UNIPROTKB:' + row['Entry']
            # Obtain the latest HGNC ID, using the gene name.
            # Ignore synonyms or obsolete gene names.
            hgnc_name = row['Gene Names'].split(' ')[0]
            # Map to the corresponding entry in the genenames.org data.
            dfobject = dfhgnc[dfhgnc['Approved symbol'].values == hgnc_name]
            # JULY 2023 - CodeID format changed to SAB:CODE.
            if dfobject.shape[0] > 0:
                #object = 'HGNC ' + dfobject['HGNC ID'].iloc[0]
                obj = dfobject['HGNC ID'].iloc[0]
                out.write(subject + '\t' + pred + '\t' + obj + '\n')

            # Jan 2025 - GO annotations
            write_go_annotation_edges(subject=subject, go_column=row['Gene Ontology (biological process)'],go_aspect='p',out=out)
            write_go_annotation_edges(subject=subject, go_column=row['Gene Ontology (cellular component)'], go_aspect='c', out=out)
            write_go_annotation_edges(subject=subject, go_column=row['Gene Ontology (molecular function)'], go_aspect='f', out=out)

    return

def write_nodes_file(df: pd.DataFrame,  ulog: UbkgLogging, sab_jkg_dir: str):

    """
    Writes a nodes file in OWLNETS format.
    :param df: DataFrame of source information
    :param ulog: UbkgLogging object
    :param sab_jkg_dir:
    :return:
    """

    # NODE METADATA
    # Write a row for each unique concept in the 'code' column.

    # Only UNIPROTKB codes need to be in the nodes file.
    # HGNC IDs are part of the UMLS data.

    node_metadata_path: str = os.path.join(sab_jkg_dir, 'OWLNETS_node_metadata.txt')
    ulog.print_and_logger_info('Building: ' + os.path.abspath(node_metadata_path))

    with open(node_metadata_path, 'w') as out:
        out.write(
            'node_id' + '\t' + 'node_namespace' + '\t' + 'node_label' + '\t' + 'node_definition' + '\t' + 'node_synonyms' + '\t' + 'node_dbxrefs' + '\n')
        # Show TQDM progress bar.
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):
            node_id = 'UNIPROTKB:' + row['Entry']
            node_namespace = 'UNIPROTKB'

            # January 2024
            # The Protein Names field formats as follows:
            # Approved name (synonym 1) (synonym 2)...
            # In other words, parentheses are used to delimit synonyms.
            # In addition, each synonym string can contain parentheses.
            # Example: 'Plasminogen receptor (KT) (Plg-R(KT))'
            # It is necessary to do the following:
            # 1. Isolate the approved name and make the approved name as the node label.
            # 2. Separate the synonyms using the outermost pairs of parentheses.
            # 3. Retain all other parentheses to prevent the creation of spurious synonyms.

            # node_label = row['Entry Name']
            # node_definition = row['Protein names']

            protein_names = row['Protein names']
            # Extract the recommended name.
            # Split on the parenthesis. This results in splits on
            # nested parentheses and thus spurious synonyms; however,
            # we only need the first element, which is the recommended name.
            approved_name = re.split(r'[()]', protein_names)
            # Remove extraneous blanks.
            for n in approved_name:
                if n.strip() == '':
                    approved_name.remove(n)
            # The recommended name is the first name in the list.
            node_label = approved_name[0]

            # Synonyms:
            # Replace the approved name with the UniProtKB Entry Name, so it will the first synonym.
            synonyms = [row['Entry Name']]
            # Parse the protein names string by level of nested parentheses.
            synonyms_parsed = uparse.parse_string_nested_parentheses(protein_names)
            if synonyms_parsed != []:
                # Treat 0-level strings as synonyms.
                for parsetuple in synonyms_parsed:
                    if parsetuple[0] == 0:
                        synonyms.append(parsetuple[1])

            # Delimit.
            node_synonyms = '|'.join(synonyms)

            node_definition = row['Function [CC]']

            node_dbxrefs = ''

            out.write(
                node_id + '\t' + node_namespace + '\t' + node_label + '\t' + node_definition + '\t' + node_synonyms + '\t' + node_dbxrefs + '\n')

    return



def getargs()->argparse.Namespace:
    # Parse command line arguments.
    parser = argparse.ArgumentParser(
        description='Builds OWLNETS files from UNIPROTKB source',
        formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--fetchnew", action="store_true", help="fetch new set of annotation files ")

    return parser.parse_args()


def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = UbkgLogging(log_dir=log_dir, log_file='ubkg.log')

    args = getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/uniprotkb/uniprotkb.ini')
    cfg = ubkgConfigParser(path=cfgpath, log_dir=log_dir, log_file='ubkg.log')

    # Get sab_source and sab_jkg directories.
    # The config file contains absolute paths to the parent directories in the local repo.
    # Affix the SAB to the paths.
    sab_source_dir = os.path.join(os.path.dirname(os.getcwd()),
                                  cfg.get_value(section='Directories', key='sab_source_dir'), 'UNIPROTKB')
    sab_jkg_dir = os.path.join(os.path.dirname(os.getcwd()), cfg.get_value(section='Directories', key='sab_jkg_dir'),
                               'UNIPROTKB')

    if not args.fetchnew:
        # Download file from UNIPROTKB.
        dfuniprotkb = getuniprotkb(cfg=cfg, ulog=ulog, sab_source_dir=sab_source_dir, sab_jkg_dir=sab_jkg_dir)
        # Get latest list of HGNC IDs from genenames.org
        dfhgnc = getallhgncid(cfg=cfg, ulog=ulog, sab_source_dir=sab_source_dir)

    else:

        # Read previously downloaded file.
        filepath = os.path.join(os.path.join(sab_jkg_dir, 'UNIPROTKB_all.tsv'))
        dfuniprotkb = uextract.read_csv_with_progress_bar(filepath, sep='\t')
        dfuniprotkb = dfuniprotkb.replace(np.nan, '', regex=True)

        # Read previously downloaded HGNC information.
        hgncpath = os.path.join(sab_jkg_dir, 'HGNC.TSV')
        dfhgnc = uextract.read_csv_with_progress_bar(hgncpath, sep='\t')
        dfhgnc = dfhgnc.replace(np.nan, '', regex=True)


    # Build OWLNETS text files.
    write_edges_file(df=dfuniprotkb, ulog=ulog, dfhgnc=dfhgnc, sab_jkg_dir=sab_jkg_dir)
    write_nodes_file(df=dfuniprotkb, sab_jkg_dir=sab_jkg_dir)

if __name__ == "__main__":
    main()
