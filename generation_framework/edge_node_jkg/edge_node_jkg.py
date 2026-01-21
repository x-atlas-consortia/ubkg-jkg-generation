#!/usr/bin/env python
# coding: utf-8

"""
Converts UBKG edge and node files to JKG JSON format.
"""

import os
import sys
import argparse
import json

import numpy as np
import pandas as pd
from io import StringIO

from tqdm import tqdm

from typing import NamedTuple

# Define a schema for rows
class RowSchema(NamedTuple):
    Index: int
    source: str
    target: str
    weight: float

# Import UBKG utilities which is in a directory that is at the same level as the script directory.
# Go "up and over" for an absolute path.
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'ubkg_utilities')
sys.path.append(fpath)

# argparser
from ubkg_args import RawTextArgumentDefaultsHelpFormatter
# Centralized logging module
from find_repo_root import find_repo_root
from ubkg_logging import ubkgLogging

# config file
from ubkg_config import ubkgConfigParser

from ubkg_extract import ubkgExtract

def getargs() -> argparse.Namespace:

    # Parse arguments.
    parser = argparse.ArgumentParser(
    description='Convert UBKG edge and node files to JKG JSON format',
    formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument("sab", help="Identifier for cell type annotation")
    args = parser.parse_args()

    return args

def getfilename(cfg:ubkgConfigParser, sab:str, filetype:str) -> str:

    filelist = cfg.get_value(section='Files', key=filetype).split(',')
    for f in filelist:
        nodefile = os.path.join(find_repo_root(),cfg.get_value(section='Directories', key='sab_jkg_dir'), sab, f)
        if os.path.exists(nodefile):
           return nodefile

    raise FileNotFoundError(f'No file found with name in list: {filelist}' )

def get_relations_ontology_nodes()-> pd.DataFrame:
    import requests

    url = "https://raw.githubusercontent.com/oborel/obo-relations/master/ro.json"

    # Use requests to fetch the content
    response = requests.get(url)
    data = response.text  # Get the raw content as a string
    df_ro = pd.read_json(StringIO(data))
    df_ro_nodes = df_ro['graphs'][0]['nodes']
    return df_ro_nodes

def no_node_value(test_str: str) ->bool:
    """
    Test for whether test_str corresponds to variant of "None" or "nan"
    :param test_str:
    :return: Boolean
    """

    return test_str == 'None' or test_str is None or test_str is np.nan

def build_nodes_array_for_nodes(node_metadata: pd.DataFrame, df_ro_nodes: pd.DataFrame, sab:str) -> list:

    """
    Builds the nodes array per the JKG schema.
    :param node_metadata: DataFrame of node metadata
    :param df_ro_nodes: DataFrame of ro nodes
    :param sab: SAB identifier
    :return: list
    """

    list_nodes = []

    for row in tqdm(node_metadata.itertuples(), desc='node_metadata'):

        node_curie=row.node_id.split('/')[-1].replace('_',':')
        node_sab = node_curie.split(':')[0]

        node_label = row.node_label
        if no_node_value(node_label):
            node_label = ''

        node_definition = row.node_definition
        if no_node_value(node_definition):
             node_definition = ''

        node_synonyms = row.node_synonyms
        if no_node_value(node_synonyms):
             node_synonyms = []
        else:
            node_synonyms = str(row.node_synonyms).split('|')

        node_dbxrefs = row.node_dbxrefs
        if no_node_value(node_dbxrefs):
             node_dbxrefs = []
        else:
            node_dbxrefs = str(row.node_dbxrefs).split('|')

        # Concept node
        dict_concept_node = {'labels': ['Concept']}
        dict_concept_prop = {
            'id': node_curie,
            'pref_term': node_label,
            'sab': node_sab
        }
        dict_concept_node['properties'] = dict_concept_prop
        list_nodes.append(dict_concept_node)

        # Term nodes
        # Preferred term
        dict_term_node ={'labels': ['Term']}
        dict_term_prop = {
            'id': node_label,
            'sab': node_sab
        }
        dict_term_node['properties'] = dict_term_prop
        list_nodes.append(dict_term_node)

        # Synonym Term nodes
        dict_syn_node ={'labels': ['Term']}
        for syn in node_synonyms:
            if syn != node_label:
                dict_syn_prop = {
                    'id': syn,
                    'sab': node_sab
                }
                dict_syn_node['properties'] = dict_syn_prop
                list_nodes.append(dict_syn_node)

        # Label definition node
        if node_definition !='':
            dict_def_node = {'labels': ['Label_Definition']}
            dict_def_prop = {
                'id': node_curie,
                'def': node_definition,
                'sab': node_sab
            }
            dict_def_node['properties'] = dict_def_prop
            list_nodes.append(dict_def_node)

        # dbxref concepts
        for c in node_dbxrefs:
            if c is not None:
                dict_dbxref_node = {'labels': ['Concept']}
                dict_dbxref_prop = {
                    'id': c,
                    'pref_term': c,
                    'sab': c.split(':')[0]
                }
                dict_dbxref_node['properties'] = dict_dbxref_prop
                list_nodes.append(dict_dbxref_node)

    return list_nodes

def build_nodes_array_for_relationships(edge_metadata: pd.DataFrame, df_ro_nodes: pd.DataFrame, sab: str)-> list:
    """
    Builds nodes that correspond to the relationships in the edge metadata
    :param edge_metadata: DataFrame of edge metadata
    :param df_ro_nodes: DataFrame of RO nodes corresponding to the edge metadata
    :param sab: SAB identifier
    :return:
    """

    list_nodes = []
    # relationship node labels
    # ISA
    dict_rel_node = {'labels': ['Label_Definition']}
    dict_rel_prop = {
        'id': sab + ':ISA',
        'def': 'Subject is a class of object.',
        'label': 'ISA',
        'sab': sab
    }
    dict_rel_prop['properties'] = dict_rel_prop
    list_nodes.append(dict_rel_node)

    # CODE
    dict_rel_node = {'labels': ['Label_Definition']}
    dict_rel_prop = {
        'id': sab + ':CODE',
        'def': 'Subject has CODE relationship with object',
        'label': 'CODE',
        'sab': sab
    }
    dict_rel_prop['properties'] = dict_rel_prop
    list_nodes.append(dict_rel_node)

    predicates = edge_metadata['predicate'].drop_duplicates().dropna()
    for p in tqdm(predicates, desc='edge_metadata'):
        if p != 'http://www.w3.org/2000/01/rdf-schema#subClassOf':
            dict_rel_node = {'labels': ['Label_Definition']}
            ro_id = p.split('/')[-1]
            # Filter the list to find the matching dictionary
            result = next((item for item in df_ro_nodes if item['id'] == p), None)
            if result:
                pid = result['id'].replace('_', ':')
                # Safely get the value from 'meta' -> 'definition' -> 'val'
                definition = result.get('meta', {}).get('definition', {}).get('val', '')
                if definition == 'None':
                    definition = ''
                lbl = result['lbl']
            else:
                pid = p
                definition = ''
                lbl = p

            dict_rel_prop = {
                'id': pid,
                'def': definition,
                'label': lbl,
                'sab': sab
            }
            dict_rel_node['properties'] = dict_rel_prop
            list_nodes.append(dict_rel_node)

    return list_nodes

def build_nodes_array_for_sources(node_metadata: pd.DataFrame)->list:
    """
    Builds the sources array per the JKG schema.
    :param node_metadata: DataFrame of node metadata
    :return: list
    """

    list_sab = []
    list_sources = []

    # Get SABs for all nodes and cross-references.
    for row in tqdm(node_metadata.itertuples(), desc='sources'):
        node_sab = row.node_id.split('/')[-1].split('_')[0]
        list_sab.append(node_sab)
        dbxrefs = str(row.node_dbxrefs)
        if dbxrefs is not None:
            dbxrefs = dbxrefs.split('|')
            for dbxref in dbxrefs:
                dbxref_sab = dbxref.split(':')[0]
                list_sab.append(dbxref_sab)

    # Build source nodes for unique sabs.
    set_sab = set(list_sab)
    for sab in set_sab:
        dict_source_node = {'labels': ['Source_Definition']}
        dict_source_prop = {
            'id': f'{sab}:{sab}',
            'name': sab,
            'sab': sab
        }
        dict_source_node['properties'] = dict_source_prop
        list_sources.append(dict_source_node)
    return list_sources

def build_evidence_dict(evidence_class:str, lowerbound:float, upperbound:float, value:float, unit:str, sab:str) ->dict:
    """
    Builds an evidence object per the JKG schema.
    :param row: row from a DataFrame
    :param sab: SAB identifier
    :return:
    """

    if no_node_value(evidence_class):
        evidence_class = ''
    if no_node_value(lowerbound):
        lowerbound = 0
    if no_node_value(value):
        value = 0
    if no_node_value(upperbound):
        upperbound = 0
    if no_node_value(unit):
        unit = ''

    return {'sab': sab,
     'evidence_class': evidence_class,
     'lowerbound': lowerbound,
     'upperbound': upperbound,
     'value': value,
     'unit': unit}

def build_rels_array_not_code(edge_metadata: pd.DataFrame, df_ro_nodes: pd.DataFrame, sab: str)->list:
    """
        Builds the nodes array per the JKG schema for relationships that are not CODE
        :param edge_metadata: DataFrame of edge metadata
        :param df_ro_nodes: DataFrame of ro nodes
        :param sab: SAB identifier
        :return: list
        """

    list_rels = []
    for row in tqdm(edge_metadata.itertuples(), desc='rels'):
        predicate = row.predicate
        subj = row.subject.split('/')[-1].replace('_',':')
        obj = row.object.split('/')[-1].replace('_',':')

        if predicate == 'http://www.w3.org/2000/01/rdf-schema#subClassOf':
            lbl = 'ISA'
        else:
            pred_id = predicate.split('_')[-1]
            lbl = pred_id
            # Filter the list to find the matching dictionary.
            result = next((item for item in df_ro_nodes if item['id'] == predicate), None)
            if result:
                lbl = result['lbl']

        if 'evidence_class' in edge_metadata.columns:
            evidence_class = row.evidence_class
            if no_node_value(evidence_class):
                evidence_class = ''
        else:
            evidence_class = ''
        evidence_dict={'evidence_class': evidence_class}

        rel={'label': lbl,
             'start':{'properties':{'id': subj}},
             'end':{'properties':{'id': obj}},
             'properties':evidence_dict
             }

        list_rels.append(rel)

    return list_rels

def build_code_rels(node_metadata: pd.DataFrame, sab: str)->list:
    """
    Build a set of CODE rel nodes.
    :param node_metadata: DataFrame of node metadata
    :param sab: SAB identifier
    :return: list
    """

    list_rels = []
    for row in tqdm(node_metadata.itertuples(), desc='rels'):

        # Obtain data to be used to build the set of CODE rels
        # for the node.

        node_id = row.node_id.split('/')[-1].replace('_',':')
        node_label = row.node_label
        node_definition = row.node_definition
        if no_node_value(node_definition):
              node_definition = ''

        node_synonyms = row.node_synonyms
        if no_node_value(node_synonyms):
             node_synonyms = []
        else:
            node_synonyms = str(row.node_synonyms).split('|')

        # Evidence class is a property of edges, not nodes.
        evidence_class = ''
        if 'lowerbound' in node_metadata.columns:
            lowerbound = row.lowerbound
            if no_node_value(lowerbound):
                lowerbound = 0
            else:
                lowerbound = float(lowerbound)
        else:
            lowerbound = 0

        if 'upperbound' in node_metadata.columns:
            upperbound = row.upperbound
            if no_node_value(upperbound):
                upperbound = 0
            else:
                upperbound = float(upperbound)
        else:
            upperbound = 0

        if 'value' in node_metadata.columns:
            val = row.value
            if no_node_value(val):
                val = 0
            else:
                val = float(val)
        else:
            val = 0

        if 'unit' in node_metadata.columns:
            unit = row.unit
            if no_node_value(unit):
                unit = ''
        else:
            unit = ''

        evidence_dict = {
            'evidence_class': evidence_class,
            'lowerbound': lowerbound,
            'upperbound': upperbound,
            'value': val,
            'unit': unit
        }

        # Build the CODE rel corresponding to the preferred term.
        start_dict = {"properties":{"id":node_id}}
        end_dict = {'id': node_label}
        rel_prop_dict = evidence_dict
        rel_prop_dict['codeid'] = node_id
        rel_prop_dict['def'] = node_definition
        rel_prop_dict['tty'] = 'PT'
        rel_prop_dict['sab'] = sab

        rel_dict = {
            'label': 'CODE',
            'start':start_dict,
            'end':end_dict,
            'properties':rel_prop_dict,
        }
        list_rels.append(rel_dict)

        # Build CODE rels for each synonym.
        for syn in node_synonyms:
            end_dict = {'id': syn}
            rel_prop_dict = {
                'codeid': node_id,
                'sab': sab,
                'tty': 'SYN'}

            syn_dict = {
                'label': 'CODE',
                'start': start_dict,
                'end': end_dict,
                'properties': rel_prop_dict
            }
            list_rels.append(syn_dict)


    return list_rels

def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = ubkgLogging(log_dir=log_dir, log_file='edge_node_jkg.log')

    args=getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'edge_node_jkg/edge_node_jkg.ini')
    cfg = ubkgConfigParser(path=cfgpath, log_dir=log_dir, log_file='edge_node_jkg.log')

    # Instantiate UbkgExtract class
    uext = ubkgExtract(log_dir=log_dir,log_file='edge_node_jkg.log')

    # Get the node file.
    nodefile = getfilename(cfg=cfg, sab=args.sab, filetype='node')
    ulog.print_and_logger_info('-- Reading node file...')
    node_metadata = uext.read_csv_with_progress_bar(path=nodefile, on_bad_lines='skip', sep='\t')

    # Get the edge file.
    edgefile = getfilename(cfg=cfg, sab=args.sab, filetype='edge')
    ulog.print_and_logger_info('-- Reading edge file...')
    edge_metadata = uext.read_csv_with_progress_bar(path=edgefile, on_bad_lines='skip', sep='\t')

    # Get Relations Ontology node information.
    df_ro_nodes = get_relations_ontology_nodes()

    # Build array of source nodes
    ulog.print_and_logger_info('---Building array of source nodes...')
    nodesarray = build_nodes_array_for_sources(node_metadata=node_metadata)
    ulog.print_and_logger_info('---Building array of node nodes...')
    # Build array of nodes
    nodesarray = nodesarray + build_nodes_array_for_nodes(node_metadata=node_metadata, df_ro_nodes=df_ro_nodes, sab=args.sab)

    # Append array of relationship nodes.
    ulog.print_and_logger_info('---Building array of relationship nodes...')
    #nodesarray = nodesarray + build_nodes_array_for_relationships(edge_metadata=edge_metadata, df_ro_nodes=df_ro_nodes, sab=args.sab)

    # Build array of rels.
    ulog.print_and_logger_info('-- Building non-CODE rels array...')
    relsarray = build_rels_array_not_code(edge_metadata=edge_metadata, df_ro_nodes=df_ro_nodes, sab=args.sab)

    # Add CODE rels
    ulog.print_and_logger_info('---Adding CODE rels to rels array...')
    relsarray = relsarray + build_code_rels(node_metadata=node_metadata, sab=args.sab)

    # Write to JKG JSON.
    outfile = os.path.join(find_repo_root(), cfg.get_value(section='Directories', key='sab_jkg_dir'), args.sab,f'{args.sab}_jkg.json')
    dict_jkg = {'nodes': nodesarray, 'rels': relsarray}

    # Write the dictionary to a JSON file.
    with open(outfile, "w") as json_file:
        json.dump(dict_jkg, json_file, indent=4)  # `indent` makes it pretty-printed

if __name__ == "__main__":
    main()