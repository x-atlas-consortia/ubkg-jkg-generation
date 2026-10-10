"""
jkg_json.py
Class that manages the JKG JSON.
"""

import os
import gc
import ijson
import polars as pl
import pandas as pd
from tqdm import tqdm
from typing import Any

# Centralized logging object
from .ubkg_logging import ubkgLogging
# Application configuration object
from .ubkg_config import ubkgConfigParser
# Progress bar wrapper for reads of large JSON files
from .progressfile import ProgressFile
from .ubkg_timer import UbkgTimer
from .ubkg_extract import ubkgExtract

from ..functions.find_repo_root import find_repo_root


class Jkgjson:

    def _load_jkg_json(self, max_nodes: int = None, max_rels: int = None):

        """
        Does the following:

        1. Loads the JKG JSON file into a set of Pandas Dataframes, in a single
        pass for:

           - Nodes
             - Source nodes
             - Node_Label nodes
             - Rel_Label nodes
             - Concept nodes
             - Term nodes
           - Relationships (rels)
             - "coderels" or concept-term (code) relationships
             - other rels (concept-concept relationships)

           The structure of DataFrames depends on the content:
           - nodes: one row per node, with 'labels' as a list column and properties flattened as columns
           - rels: one row per relationship, with start/end/properties flattened as columns

        2. To conserve memory, exports DataFrames to temporary files.

        :param max_nodes: maximum number of nodes to load. None loads all nodes.
        :param max_rels: maximum number of rels to load. None loads all rels.

        Note that because the nodes array is before the rels array in the JKG JSON,
        the entire nodes array will be read, even if max_nodes is set.
        This is primarily for debugging purposes to limit the read time of large
        JKG JSON files.


        """

        self.log.print_and_logger_info('*** READING JKG JSON FILE ***')
        # Treat 0 or None as "load all".
        max_nodes = None if not max_nodes else max_nodes
        max_rels = None if not max_rels else max_rels

        if max_nodes is not None and max_rels is not None:
            self.log.print_and_logger_info(f'Reading only {max_nodes} nodes and {max_rels} rels from {self.jkg_json_filename}.')

        # Build the full path to JKG JSON file.
        jkg_json_full = os.path.join(self.jkg_json_dir, self.jkg_json_filename)
        if not(os.path.exists(jkg_json_full)):
            self.log.print_and_logger_error(f"JKG JSON file {jkg_json_full} not found.")
            exit(1)
        # Get the file size of JKG JSON for tqdm.
        self.file_size = os.path.getsize(jkg_json_full)

        # Node types:
        # - Source
        # - Node_Label
        # - Rel_Label
        # - Term
        # - Concept
        source_node_rows = []
        node_label_node_rows = []
        rel_label_node_rows = []
        concept_node_rows = []
        term_node_rows = []

        # Rel types
        # These are not specified explicitly in the JKG Schema.
        # - coderels--concept-term (code) relationships
        # - rels--concept-concept relationships
        code_rel_rows = []
        rel_rows = []

        with open(jkg_json_full, "rb") as f:

            with tqdm(desc=f"Reading from {self.jkg_json_filename}",
                      total=self.file_size,
                      unit="B", unit_scale=True, unit_divisor=1024) as pbar:

                # Wrap the ijson streaming read with a progress bar.
                pf = ProgressFile(f, pbar)

                # Iterate parse events and use ijson.ObjectBuilder to reconstruct each item.
                builder = None
                current_key = None

                for prefix, event, value in ijson.parse(pf):

                    # Stop early if both limits are reached.
                    num_nodes = len(source_node_rows) \
                                + len(node_label_node_rows) \
                                + len(rel_label_node_rows) \
                                +len(concept_node_rows) \
                                + len(term_node_rows)
                    num_rels = len(code_rel_rows) + len(rel_rows)

                    nodes_done = max_nodes is not None and num_nodes >= max_nodes
                    rels_done = max_rels is not None and num_rels >= max_rels
                    if nodes_done and rels_done:
                        break

                    # Detect which top-level array we are in.
                    if prefix == "nodes" and event == "start_array":
                        current_key = "nodes"
                    elif prefix == "rels" and event == "start_array":
                        current_key = "rels"

                    # Skip building if this array's limit is already reached.
                    if prefix == "nodes.item" and event == "start_map":
                        if max_nodes is None or num_nodes < max_nodes:
                            builder = ijson.ObjectBuilder()
                    elif prefix == "rels.item" and event == "start_map":
                        if max_rels is None or num_rels < max_rels:
                            builder = ijson.ObjectBuilder()

                    if builder is not None:
                        # Process the node/rel information.
                        builder.event(event, value)

                        # End of the current item.
                        # Flatten the item into a row.

                        if event == "end_map" and prefix in ("nodes.item", "rels.item"):
                            item = builder.value
                            if prefix == "nodes.item":

                                # Flatten the properties object using the unpacking operator.
                                properties = item.get("properties", {})
                                row = {
                                    "labels": item.get("labels", []),
                                    **{f"properties_{k}": v for k, v in properties.items()}
                                }
                                # Split node objects by type.
                                labels = item.get("labels", [])
                                if "Source" in labels:
                                    source_node_rows.append(row)
                                elif "Node_Label" in labels:
                                    node_label_node_rows.append(row)
                                elif "Rel_Label" in labels:
                                    rel_label_node_rows.append(row)
                                elif "Concept" in labels:
                                    concept_node_rows.append(row)
                                elif "Term" in labels:
                                    term_node_rows.append(row)
                                else:
                                    raise Exception(f"Unknown node type: {labels}")

                            elif prefix == "rels.item":
                                properties = item.get("properties", {})
                                # Flatten the properties object using the unpacking operator.
                                row = {
                                    "label": item.get("label"),
                                    "start_id": item.get("start", {}).get("properties", {}).get("id"),
                                    "end_id": item.get("end", {}).get("properties", {}).get("id"),
                                    **{f"properties_{k}": v for k, v in properties.items()}
                                }
                                # Split coderels from other rels.
                                label = item.get("label","")
                                if label=="CODE":
                                    code_rel_rows.append(row)
                                else:
                                    rel_rows.append(row)

                            builder = None

            self.log.print_and_logger_info(f'*** JKG JSON LOAD SUMMARY:')
            self.log.print_and_logger_info('* NODE OBJECTS')
            self.log.print_and_logger_info(f"---- Source nodes: {len(source_node_rows):,}")
            self.log.print_and_logger_info(f"---- Node_Label nodes: {len(node_label_node_rows):,}")
            self.log.print_and_logger_info(f"---- Relation_Label nodes: {len(rel_label_node_rows):,}")
            self.log.print_and_logger_info(f"---- Concept nodes: {len(concept_node_rows):,}")
            self.log.print_and_logger_info(f"---- Term nodes: {len(term_node_rows):,}")
            self.log.print_and_logger_info('* REL OBJECTS')
            self.log.print_and_logger_info(f"---- non-CODE rels: {len(rel_rows):,}")
            self.log.print_and_logger_info(f"---- CODE rels: {len(code_rel_rows):,}")

            """
            To conserve memory, export subsets of JKG JSON to temporary files. 
            
            To reduce memory pressure, export subsets in order of size of export 
            rather than the order in which they appear in the JKG JSON file:
            1. CODE rels
            2. rels
            3. Concept nodes
            4. Term nodes
            5. Rel_Label nodes
            6. Source nodes
            7. Node_Label nodes
            
            Note that the order by size is almost the exact reverse of the order of appearance.
            """

            # CODE rels
            self._export_coderels(rows=code_rel_rows)

            # non-CODE rels
            self._export_non_coderels(rows=rel_rows)

            # Concept nodes
            self._export_concept_nodes(rows=concept_node_rows)

            # Term nodes
            self._export_term_nodes(rows=term_node_rows)

            # Rel_Label nodes
            self._export_rel_label_nodes(rows=rel_label_node_rows)

            # Source nodes
            self._export_source_nodes(rows=source_node_rows)

            # Node_Label nodes
            self._export_node_label_nodes(rows=node_label_node_rows)

    def _export_coderels(self, rows:list):
        """
        Export coderels DataFrame to temporary file.

        :param rows: list of rows to export

        Special processing: delete MTH:NOCODE rels

        """
        utimer = UbkgTimer(display_msg="Building CODE rels DataFrame")
        self.coderels = pd.DataFrame(rows).fillna('')
        utimer.stop()

        # Unload CODE rel list from memory.
        self._unload_item(item_to_unload=rows)

        # Remove MTH:NOCODE rels
        utimer = UbkgTimer(display_msg="Deleting MTH:NOCODE CODE rels")
        self.coderels = self.coderels.loc[self.coderels['properties_codeid'] != 'MTH:NOCODE'].copy()
        utimer.stop()

        self.log.print_and_logger_info('Exporting CODE rels DataFrame to temporary file')
        self._export_unload_dataframe(dfexport=self.coderels, filename='coderels')

        # Unload DataFrame from memory.
        self._unload_item(item_to_unload=self.coderels)

    def _export_non_coderels(self, rows:list):
        """
        Export non-coderels DataFrame to temporary file.
        :param rows: list of rows to export
        """
        utimer = UbkgTimer(display_msg="Building non-CODE rels DataFrame")
        self.rels = pd.DataFrame(rows).fillna('')
        utimer.stop()

        # Unload rel list from memory.
        self._unload_item(item_to_unload=rows)

        self.log.print_and_logger_info('Exporting non-CODE rels DataFrame to temporary file')
        self._export_unload_dataframe(dfexport=self.rels, filename='rels')

        # Unload DataFrame from memory.
        self._unload_item(item_to_unload=self.rels)

    def _export_concept_nodes(self, rows:list):
        """
        Export concept nodes DataFrame to temporary file.
        :param rows: list of rows to export

        """
        utimer = UbkgTimer(display_msg="Building Concept nodes DataFrame")
        self.concept_nodes = pd.DataFrame(rows).fillna('')
        utimer.stop()

        # Unload node list from memory.
        self._unload_item(item_to_unload=rows)

        self.log.print_and_logger_info('Exporting Concept nodes DataFrame to temporary file')
        self._export_unload_dataframe(dfexport=self.concept_nodes, filename='concept_nodes')

        # Unload DataFrame from memory.
        self._unload_item(item_to_unload=self.concept_nodes)

    def _export_term_nodes(self, rows:list):
        """
        Export term nodes DataFrame to temporary file.
        Special processing: drop duplicates.

        :param rows: list of rows to export
        """
        utimer = UbkgTimer(display_msg="Building Term nodes DataFrame")

        # When creating DataFrame, drop duplicate term nodes.
        self.term_nodes = pd.DataFrame(rows).fillna('').drop_duplicates('properties_id')
        utimer.stop()

        # Unload node list from memory.
        self._unload_item(item_to_unload=rows)

        self.log.print_and_logger_info('Exporting Term nodes DataFrame to temporary file')
        self._export_unload_dataframe(dfexport=self.term_nodes, filename='term_nodes')

        # Unload DataFrame from memory.
        self._unload_item(item_to_unload=self.term_nodes)

    def _export_rel_label_nodes(self, rows:list):
        """
        Export rel label nodes DataFrame to temporary file.
        :param rows: list of rows to export

        """
        utimer = UbkgTimer(display_msg="Building Rel_Label nodes DataFrame")
        self.rel_label_nodes = pd.DataFrame(rows).fillna('')
        utimer.stop()

        # Unload node list from memory.
        self._unload_item(item_to_unload=rows)

        self.log.print_and_logger_info('Exporting Rel_Label nodes DataFrame to temporary file')
        self._export_unload_dataframe(dfexport=self.rel_label_nodes, filename='rel_label_nodes')

        # Unload DataFrame from memory.
        self._unload_item(item_to_unload=self.rel_label_nodes)

    def _export_source_nodes(self, rows:list):
        """
        Export source nodes DataFrame to temporary file.
        :param rows: list of rows to export

        """
        utimer = UbkgTimer(display_msg="Building Source nodes DataFrame")
        self.source_nodes = pd.DataFrame(rows).fillna('')
        utimer.stop()

        # Unload node list from memory.
        self._unload_item(item_to_unload=rows)

        self.log.print_and_logger_info('Exporting Source nodes DataFrame to temporary file')
        self._export_unload_dataframe(dfexport=self.source_nodes, filename='source_nodes')

        # Unload DataFrame from memory.
        self._unload_item(item_to_unload=self.source_nodes)

    def _export_node_label_nodes(self, rows:list):
        """
        Export node label nodes DataFrame to temporary file.
        :param rows: list of rows to export

        """
        utimer = UbkgTimer(display_msg="Building Node_Label nodes DataFrame")
        self.node_label_nodes = pd.DataFrame(rows).fillna('')
        utimer.stop()

        # Unload node list from memory.
        self._unload_item(item_to_unload=rows)

        self.log.print_and_logger_info('Exporting Node_Label nodes DataFrame to temporary file')
        self._export_unload_dataframe(dfexport=self.node_label_nodes, filename='node_label_nodes')

        # Unload DataFrame from memory.
        self._unload_item(item_to_unload=self.node_label_nodes)


    def _export_unload_dataframe(self, dfexport:pd.DataFrame, filename: str):
        """
        Exports a DataFrame to file and frees memory.

        The use case is a DataFrame that is a subset of nodes from the JKG JSON file.
        :param dfexport: DataFrame
        :param filename: name of the export file, without extension
        """
        #if filename == 'concept_nodes':
            #outfile = os.path.join(self.jkg_json_dir, filename + '.tsv')
            #self.uextract.to_csv_with_progress_bar(df=dfexport, path=outfile, sep='\t',index=False)
            #print('DEBUG EXIT')
            #exit(1)

        outfile = os.path.join(self.jkg_json_dir, filename + '.parquet')
        self.uextract.to_parquet_with_progress_bar(df=dfexport, outpath=outfile)

        self._unload_item(item_to_unload=dfexport)

    def load_dataframe(self,filename:str)->pd.DataFrame:
        """
        Imports a DataFrame from a temporary file.
        :param filename: name of the temporary file, without extension
        :return: DataFrame
        """

        infile = os.path.join(self.jkg_json_dir, filename + '.parquet')
        self.log.print_and_logger_info(f'Loading DataFrame from {infile}')
        #return self.uextract.read_csv_with_progress_bar(path=infile, sep='\t').fillna('')
        return self.uextract.read_parquet_with_progress_bar(path=infile)

    def _unload_item(self, item_to_unload:Any):
        """
        Explicitly unloads an object from memory.
        :param item_to_unload: object to be unloaded

        """

        self.log.print_and_logger_info(f'Unloading item...')
        if type(item_to_unload) is list:
            item_to_unload.clear()
        if type(item_to_unload) is pd.DataFrame:
            item_to_unload = None

        gc.collect()

    def __init__(self, log: ubkgLogging, cfg: ubkgConfigParser,
                 max_nodes: int=0, max_rels: int=0) -> None:
        self.log = log
        self.cfg = cfg

        # Get path to the JKG JSON.
        self.jkg_json_dir = cfg.get_value(section='jkg_json',key='jkg_json_dir')
        self.jkg_json_filename = cfg.get_value(section='jkg_json',key='jkg_json_filename')
        self.jkg_schema_filename = cfg.get_value(section='jkg_json',key='jkg_schema_filename')

        # For exporting and importing DataFrames of subsets of the JKG JSON.
        self.uextract = ubkgExtract(ulog=self.log)

        # Read the JKG JSON file and separate the different types of nodes and rels into DataFrames.
        self._load_jkg_json(max_nodes=max_nodes, max_rels=max_rels)








