#!/usr/bin/env python
# coding: utf-8

# Converts to OWLNETS format the CSV file downloaded (as a GZIP archive) from BioPortal for an
# ontology to OWLNETS format.

# This script is designed to align with the conversion logic executed in the build_csv.py script--e.g., outputs to
# owlnets_output, etc. This means: 1. The CSV file will be extracted from GZ and downloaded to the OWL folder
# path, even though it is not an OWL file. 2. The OWLNETS output will be stored in the OWLNETS folder path.

import argparse
import pandas as pd
import numpy as np
import os
import sys
import urllib
from urllib.request import Request
import requests

# Import UBKG utilities which is in a directory that is at the same level as the script directory.
# Go "up and over" for an absolute path.
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath,'generation_framework/utilities')
sys.path.append(fpath)
# Extraction module
from classes.ubkg_extract import ubkgExtract

# argparser
from classes.ubkg_args import RawTextArgumentDefaultsHelpFormatter
# Centralized logging module
from functions.find_repo_root import find_repo_root
from classes.ubkg_logging import ubkgLogging

# config file
from classes.ubkg_config import ubkgConfigParser


# UBKG-JGKG standardization object
from classes.ubkg_standardizer import ubkgStandardizer

def getAPIKey(ulog:ubkgLogging)->str:

    # Get an API key from a text file in the application directory.  (The file should be excluded from github via .gitignore.)
    # (To obtain an API key, create an account with NCBO. The API key is part of the account profile.)
    try:
        fapikey = open(os.path.join(os.getcwd(), 'translators/gzip_csv2jkgen/apikey.txt'), 'r')
        apikey = fapikey.read()
        fapikey.close()
    except FileNotFoundError as e:
        ulog.print_and_logger_info('Missing file: apikey.txt')
        exit(1)

    return apikey

def translate_sab_property_label_to_ro_iri(ulog:ubkgLogging, apikey: str, sab: str, label: str)-> tuple[str,str]:

    """
    Translates the label for a property from a SAB into the corresponding property from the Relations Ontology (RO).
    :param ulog: ubkgLogging object
    :param apikey: API key used to call the NCBO REST API
    :param sab: SAB
    :param label: label for a property in the ontology referenced by the SAB
    :return: a tuple of (IRI, label) from RO.
    """

    # Example returns:
    # sab=XCO label=has_component translates to RO_0002211 (regulates)
    # sab=PR label=has_component translates to RO_0002180 (has_component)

    propIRI = ''
    proplbl = ''

    # Assume simple URL encoding for the colum header.
    label = label.replace('_', '%20')

    # Obtain from NCBO API the property IRI corresponding to sab:label.
    urlNCBO = 'https://data.bioontology.org/property_search?apikey=' + apikey + '&q=' + label + '&ontologies=' + sab + '&require_exact_match=true'
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    response = requests.get(urlNCBO, headers=headers)
    if response.status_code == 200:
        responsejson = response.json()
        totalCount = responsejson.get('totalCount')
        if totalCount is not None and totalCount > 0:
            prop = responsejson.get('collection')[0]
            propIRI = prop.get('@id')

    # Obtain corresponding property label from RO.json.
    if propIRI != '':
        urlRO = 'https://raw.githubusercontent.com/oborel/obo-relations/master/ro.json'
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        response = requests.get(urlRO, headers=headers)
        if response.status_code == 200:
            responsejson = response.json()
            all_graphs = responsejson.get('graphs')
            if all_graphs is not None and len(all_graphs) > 0:
                graphs = all_graphs[0]
                nodes = graphs.get('nodes')
                for node in nodes:
                    id = node.get('id')
                    if id == propIRI:
                        proplbl = node.get('lbl')

    ulog.print_and_logger_info(f'In {sab}, the has_component property translates to {propIRI} ({proplbl}).')
    return (propIRI, proplbl)

def get_ro_property(ulog: ubkgLogging, sab: str, col_header: str)-> tuple[str, str]:

    """
    Return information on the property in Relationship Ontology (RO) that corresponds to a column
    in a CSV file.

    The column header corresponds to the label of a relationship property in the ontology
    represented by the CSV.

    :param ulog: ubkgLogging object
    :param sab: SAB
    :param col_header: column header
    :return:
    """

    # In general, two ontologies with the same labeled property may correspond to different RO properties.

    # For example, the "has_component" property in the Experimental Conditions Ontology (XCO)
    # corresponds to RO_0002211 (regulates); however, in PR, has_component maps to RO_002180.

    # Arguments:
    # sab - identifier for the ontology in NCBO
    # col_header - string that corresponds to a column header in a NCBO OWL CSV file.

    # Obtain the property's RO IRI with a call to the NCBO API.
    # Obtain an api key for the NCBO API.
    apikey = getAPIKey(ulog=ulog)
    # Translate to an RO property.
    return translate_sab_property_label_to_ro_iri(ulog=ulog, apikey=apikey,sab=sab,label=col_header)


def getargs()->argparse.Namespace:

    # Parse command line arguments.
    parser = argparse.ArgumentParser(
    description='Convert the CSV file of the ontology (of which the URL is the required parameter) ontology to JKG Edge/Node (JKGEN) format .\n'
                'In general you should not have the change any of the optional arguments.',
    formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument("sab", help="SAB for metadata field ontology")

    # positional arguments
    parser.add_argument("-f", "--fetchnew", action="store_true", help="fetch new set of files ")

    args = parser.parse_args()
    return args

def write_edges_file(ulog: ubkgLogging, df:pd.DataFrame,
                     sab_jkg_dir: str, has_component_lbl: str,
                     sab:str, ustand:ubkgStandardizer,
                     uext:ubkgExtract):
    """

    :param ulog: ubkgLogging object
    :param df: DataFrame of source data
    :param sab_jkg_dir: output directory
    :param has_component_lbl: label for the 'has_component' relationship
    :param sab: SAB
    :param ustand: ubkgStandardizer object
    :param uext: ubkgExtract object
    :return:
    """

    """
    Assumptions about the CSV file that is the source of the DataFrame:
    1. The identifiers in the following columns are IRIs that are compliant with OBO principle 3
        --e.g.,http://purl.obolibrary.org/obo/XCO_0000121
        a. Class ID
        b. Parents
        c. has_component
     2. The IRIs in the Parents column are pipe-delimited

     The OWLNETS format represents ontology data in a TSV in format:

     subject <tab> predicate <tab> object
    
     where:
       subject - code for node in custom ontology
       predicate - a single relationship other than subClassOf--e.g., 'has_component'
       object: another code in the custom ontology

      (In canonical OWLNETS, the relationship is a IRI for a relation
      property in a standard OBO ontology, such as RO.) For custom
      ontologies such as HuBMAP, we use custom relationship strings.)
    """

    edgelist_path: str = os.path.join(sab_jkg_dir, 'jkg_edge.tsv')
    ulog.print_and_logger_info(f'Building: {os.path.abspath(edgelist_path)}')

    # Standardize subject column
    df['subject'] = ustand.standardize_code(df['Class ID'], sab=sab)

    rows = []

    # subClassOf=isa relationships from Parents column
    dfparents = df[df['Parents'].notna() & (df['Parents'].astype(str) != 'nan')].copy()

    if not dfparents.empty:
        # Assign a row per parent
        dfparents = dfparents.assign(Parents=dfparents['Parents'].str.split('|')).explode('Parents')
        dfparents = dfparents[dfparents['Parents'] != '']
        # Standardize the object code
        dfparents['object'] = ustand.standardize_code(dfparents['Parents'], sab=sab)
        dfparents['predicate'] = 'isa'
        rows.append(dfparents[['subject', 'predicate', 'object']])

    #dfparents.to_csv(edgelist_path, sep='\t', index=False)
    # has_component relationships
    if 'has_component' in df.columns:
        dfcomp = df[df['has_component'].notna() & (df['has_component'].astype(str) != 'nan')].copy()
        if not dfcomp.empty:
            dfcomp['object'] = ustand.standardize_code(dfcomp['has_component'], sab=sab)
            dfcomp['predicate'] = has_component_lbl
            rows.append(dfcomp[['subject', 'predicate', 'object']])

    # Combine and write
    if rows:
        dfedges: pd.DataFrame = pd.concat(rows, ignore_index=True)
    else:
        dfedges = pd.DataFrame(columns=['subject', 'predicate', 'object'])

    uext.to_csv_with_progress_bar(df=dfedges, path=edgelist_path, sep='\t', index=False)

def write_nodes_file(ulog: ubkgLogging, df:pd.DataFrame, sab_jkg_dir: str, sab:str, ustand:ubkgStandardizer, uext:ubkgExtract):

    """
    Writes a nodes file in OWLNETS format.
    :param ulog: ubkgLogging object
    :param df: DataFrame of source information
    :param sab_jkg_dir: output directory
    :param sab: SAB,
    :param ustand: ubkgStandardizer object
    :param uext: ubkgExtractor object
    :return:
    """

    # NODE METADATA
    # Write a row for each unique concept in the 'code' column.

    node_metadata_path: str = os.path.join(sab_jkg_dir, 'jkg_node.tsv')
    ulog.print_and_logger_info(f'Building: {os.path.abspath(node_metadata_path)}')

    # Work on a copy
    dfout: pd.DataFrame = df.copy()

    # Standardize node IDs vectorized
    dfout['node_id'] = ustand.standardize_code(dfout['Class ID'], sab)

    # Node label
    dfout['node_label'] = dfout['Preferred Label'].astype(str)

    # Node definition - handle different column names
    if 'definition' in dfout.columns:
        dfout['node_definition'] = dfout['definition'].astype(str)
    elif 'Definitions' in dfout.columns:
        dfout['node_definition'] = dfout['Definitions'].astype(str)

    # Node synonyms - handle different column names
    if 'has_exact_synonym' in dfout.columns:
        dfout['node_synonyms'] = dfout['has_exact_synonym'].astype(str)
    elif 'Synonyms' in dfout.columns:
        dfout['node_synonyms'] = dfout['Synonyms'].astype(str)

    # Node dbxrefs - handle different column names
    if 'database_cross_reference' in dfout.columns:
        dfout['node_dbxrefs'] = dfout['database_cross_reference'].astype(str)
    elif 'http://www.geneontology.org/formats/oboInOwl#hasDbXref' in dfout.columns:
        dfout['node_dbxrefs'] = dfout['http://www.geneontology.org/formats/oboInOwl#hasDbXref'].astype(str)
    else:
        dfout['node_dbxrefs'] = 'None'

    # Replace nan with 'None'
    dfout['node_synonyms'] = dfout['node_synonyms'].replace('nan', 'None').fillna('None')
    dfout['node_dbxrefs'] = dfout['node_dbxrefs'].replace('nan', 'None').fillna('None')

    # Write to file
    #dfout[['node_id', 'node_label', 'node_definition', 'node_synonyms', 'node_dbxrefs']].to_csv(
        #node_metadata_path, sep='\t', index=False
    #)
    dfout= dfout[['node_id', 'node_label', 'node_definition', 'node_synonyms', 'node_dbxrefs']]
    uext.to_csv_with_progress_bar(df=dfout,path=node_metadata_path, sep='\t', index=False)


def getdfcsv(ulog: ubkgLogging, uext: ubkgExtract, sab_source_dir: str, csv_file: str) -> pd.DataFrame:
    """
    Read and prepare CSV file.
    :param ulog: ubkgLogging object
    :param uext: UbkgExtract object
    :param sab_source_dir: path to the sab source file for the ontology
    :param csv_file: file name of the CSV
    :return: DataFrame of source information
    """

    csv_path = os.path.join(sab_source_dir, csv_file)
    ulog.print_and_logger_info(f'Reading {csv_path}...')
    dfontology = uext.read_csv_with_progress_bar(csv_path, on_bad_lines='skip', encoding='utf-8', sep=',')
    dfontology = dfontology.replace({'None': np.nan})
    dfontology = dfontology.replace({'': np.nan})

    return dfontology

def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    # The log directory location is determined by the pkt-kg package
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = ubkgLogging(log_dir=log_dir, log_file='gzip_csv2jkgen.log')

    # Obtain runtime arguments.
    args = getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/translators/gzip_csv2jkgen/gzip_csv2jkgen.ini')
    cfg = ubkgConfigParser(path=cfgpath, ulog=ulog)

    # Get sab_source and sab_jkg directories.
    # The config file contains absolute paths to the parent directories in the local repo.
    # Affix the SAB to the paths.
    sab_source_dir = os.path.join(os.path.dirname(os.getcwd()),
                                  cfg.get_value(section='Directories', key='sab_source_dir'), args.sab)
    sab_jkg_dir = os.path.join(os.path.dirname(os.getcwd()), cfg.get_value(section='Directories', key='sab_jkg_dir'),
                               args.sab)

    # Create OWLNETS related directories.
    os.makedirs(sab_source_dir, exist_ok=True)
    os.makedirs(sab_jkg_dir, exist_ok=True)

    # Set file names for downloads.
    zip_filename = args.sab + '.GZ'
    csv_filename = args.sab + '.CSV'

    owl_url = cfg.get_value(section='URL', key=args.sab)

    # Instantiate UbkgExtract class
    uext = ubkgExtract(ulog=ulog)

    # Download GZipped file and extract the CSV.
    if args.fetchnew:
        uext.get_gzipped_file(zip_url=owl_url, zip_path=sab_source_dir, extract_path=sab_source_dir, zipfilename=zip_filename,outfilename=csv_filename)

    # Load the CSV file.
    dfontology = getdfcsv(ulog=ulog, uext=uext, sab_source_dir=sab_source_dir,csv_file=csv_filename)

    # Obtain the ontology-specific property that corresponds to the 'has_component' column.
    has_component_tuple=get_ro_property(ulog=ulog, sab=args.sab, col_header='has_component')
    has_component_IRI = has_component_tuple[0]
    has_component_lbl = has_component_tuple[1]

    if has_component_IRI == '':
        # Use default from RO.
        has_component_IRI = 'RO_002180'
        has_component_lbl = 'has_component'

    # Build OWLNETS text files.
    ustand = ubkgStandardizer(ulog=ulog, repo_root=repo_root)
    write_edges_file(ulog=ulog, df=dfontology,sab_jkg_dir=sab_jkg_dir,
                     has_component_lbl=has_component_lbl,
                     sab=args.sab, ustand=ustand, uext=uext)
    write_nodes_file(ulog=ulog, df=dfontology,sab_jkg_dir=sab_jkg_dir,
                     sab=args.sab, ustand=ustand, uext=uext)


if __name__ == "__main__":
    main()

