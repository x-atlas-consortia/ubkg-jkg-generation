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

# Centralized logging object
from .ubkg_logging import ubkgLogging
# Application configuration object
from .ubkg_config import ubkgConfigParser
# Progress bar wrapper for reads of large JSON files
from .progressfile import ProgressFile
from .ubkg_timer import UbkgTimer
from ..functions.find_repo_root import find_repo_root


class Jkgjson:

    def _load_jkg_json_two_pass(self, nodetype:str):
        """
        Loads the JKG JSON file into two Polars DataFrames using two streaming
        passes with ijson.items().

        The JKG JSON conforms to a nested schema (nodes and rels), which Polars
        cannot flatten. This function loads the nested schema into two dataframes.
        Because both the JKG JSON is large and ingestion works with each
        array in sequences, this function only builds one dataframe at a time.

        :param nodetype: type of array to load - nodes or rels

        Sets either:
        - self.jkg_nodes: one row per node, with 'labels' as a list column
                          and node properties flattened as columns
        - self.jkg_rels:  one row per relationship, with start/end/properties flattened

        """

        # Full path to JKG JSON file
        jkg_json_full = os.path.join(self.jkg_json_dir, self.jkg_json_filename)
        # File size of JKG JSON for tqdm
        file_size = os.path.getsize(jkg_json_full)

        if nodetype == "node":

            """
            --- NODES ---
            Each node: { "labels": [...], "properties": { "id": ..., "sab": ..., ... } }

            """

            node_rows = []
            with open(jkg_json_full, "rb") as f:
                with tqdm(desc=f"Reading nodes from {self.jkg_json_filename}",
                        total=file_size,
                        unit="B", unit_scale=True, unit_divisor=1024) as pbar:
                    pf = ProgressFile(f, pbar)
                    for node in ijson.items(pf, "nodes.item"):
                        row = {"labels": node.get("labels", [])}
                        row.update(node.get("properties", {}))
                        node_rows.append(row)

            utimer = UbkgTimer(display_msg="Loading nodes DataFrame")
            self.jkg_nodes = pl.DataFrame(node_rows, infer_schema_length=len(node_rows))
            utimer.stop()

        else:

            """
            --- RELS ---
            Each rel: { "label": ..., "start": { "properties": { "id": ... } },
                         "end":   { "properties": { "id": ... } },
                         "properties": { "sab": ..., ... } }
            """

            rel_rows = []
            with open(jkg_json_full, "rb") as f:
                with tqdm(desc=f"Reading rels from {self.jkg_json_filename}",
                        total=file_size,
                        unit="B", unit_scale=True, unit_divisor=1024) as pbar:
                    pf = ProgressFile(f, pbar)
                    for rel in ijson.items(pf, "rels.item"):
                        row = {
                            "label": rel.get("label"),
                            "start_id": rel.get("start", {}).get("properties", {}).get("id"),
                            "end_id": rel.get("end", {}).get("properties", {}).get("id"),
                        }
                        row.update(rel.get("properties", {}))
                        rel_rows.append(row)

            utimer = UbkgTimer(display_msg="Loading rels DataFrame")
            self.jkg_rels = pl.DataFrame(rel_rows, infer_schema_length=len(rel_rows))
            utimer.stop()

    def _load_jkg_json(self, max_nodes: int = None, max_rels: int = None):

        """
        Loads the JKG JSON file into a set of Pandas dataframes, in a single
        pass for:
        1. Source nodes
        2. Node_Label nodes
        3. Rel_Label nodes
        4. Concept nodes
        5. Term nodes
        6. "coderels" or concept-term (code) relationships
        7. other rels (concept-concept relationshiops)

        :param max_nodes: maximum number of nodes to load. None loads all nodes.
        :param max_rels: maximum number of rels to load. None loads all rels.

        Note that because the nodes array is before the rels array in the JKG JSON,
        the entire nodes array will be read, even if max_nodes is set.
        This is primarily for debugging purposes to limit the read time of large
        JKG JSON files.

        Builds two types of DataFrames:
        - nodes: one row per node, with 'labels' as a list column and properties flattened as columns
        - rels: one row per relationship, with start/end/properties flattened as columns

        """

        self.log.print_and_logger_info('*** LOADING JKG JSON FILE ***')
        # Treat 0 or None as "load all".
        max_nodes = None if not max_nodes else max_nodes
        max_rels = None if not max_rels else max_rels

        if max_nodes is not None and max_rels is not None:
            self.log.print_and_logger_info(f'Loading only {max_nodes} nodes and {max_rels} rels from {self.jkg_json_filename}.')

        # Build the full path to JKG JSON file.
        jkg_json_full = os.path.join(self.jkg_json_dir, self.jkg_json_filename)
        # Get the file size of JKG JSON for tqdm
        file_size = os.path.getsize(jkg_json_full)

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
                      total=file_size,
                      unit="B", unit_scale=True, unit_divisor=1024) as pbar:

                # Wrap the ijson streaming read with a progress bar.
                pf = ProgressFile(f, pbar)

                # Iterate parse events and use ijson.ObjectBuilder to reconstruct each item.
                builder = None
                current_key = None

                for prefix, event, value in ijson.parse(pf):

                    # Stop early if both limits are reached.
                    nodes_done = max_nodes is not None and len(node_rows) >= max_nodes
                    rels_done = max_rels is not None and len(rel_rows) >= max_rels
                    if nodes_done and rels_done:
                        break

                    # Detect which top-level array we are in.
                    if prefix == "nodes" and event == "start_array":
                        current_key = "nodes"
                    elif prefix == "rels" and event == "start_array":
                        current_key = "rels"

                    # Skip building if this array's limit is already reached.
                    if prefix == "nodes.item" and event == "start_map":
                        if max_nodes is None or len(node_rows) < max_nodes:
                            builder = ijson.ObjectBuilder()
                    elif prefix == "rels.item" and event == "start_map":
                        if max_rels is None or len(rel_rows) < max_rels:
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

            utimer = UbkgTimer(display_msg="Loading nodes")

            self.source_nodes = pd.DataFrame(source_node_rows).fillna('')
            source_node_rows.clear()
            gc.collect()

            self.node_label_nodes = pd.DataFrame(node_label_node_rows).fillna('')
            node_label_node_rows.clear()
            gc.collect()

            self.rel_label_nodes = pd.DataFrame(rel_label_node_rows).fillna('')
            rel_label_node_rows.clear()
            gc.collect()

            self.concept_nodes = pd.DataFrame(concept_node_rows).fillna('')
            concept_node_rows.clear()
            gc.collect()

            self.term_nodes = pd.DataFrame(term_node_rows).fillna('')
            term_node_rows.clear()
            gc.collect()

            utimer.stop()

            utimer = UbkgTimer(display_msg="Loading non-CODE rels")
            self.rels = pd.DataFrame(rel_rows).fillna('')
            rel_rows.clear()
            utimer.stop()

            utimer = UbkgTimer(display_msg="Loading CODE rels")
            self.coderels = pd.DataFrame(code_rel_rows).fillna('')
            code_rel_rows.clear()
            utimer.stop()

            self.log.print_and_logger_info(f'JKG JSON LOAD SUMMARY:')
            self.log.print_and_logger_info('- NODE OBJECTS')
            self.log.print_and_logger_info(f"-- Source nodes: {len(self.source_nodes)}")
            self.log.print_and_logger_info(f"-- Node_Label nodes: {len(self.node_label_nodes)}")
            self.log.print_and_logger_info(f"-- Relation_Label nodes: {len(self.rel_label_nodes)}")
            self.log.print_and_logger_info(f"-- Concept nodes: {len(self.concept_nodes)}")
            self.log.print_and_logger_info(f"-- Term nodes: {len(self.term_nodes)}")
            self.log.print_and_logger_info('- REL OBJECTS')
            self.log.print_and_logger_info(f"-- non-CODE rels: {len(self.rels)}")
            self.log.print_and_logger_info(f"-- CODE rels: {len(self.coderels)}")
            self.log.print_and_logger_info(f"*** JKG JSON load complete ***")

    def __init__(self, log: ubkgLogging, cfg: ubkgConfigParser,
                 max_nodes: int=0, max_rels: int=0) -> None:
        self.log = log
        self.cfg = cfg

        # Get path to the JKG JSON.
        self.jkg_json_dir = cfg.get_value(section='jkg_json',key='jkg_json_dir')
        self.jkg_json_filename = cfg.get_value(section='jkg_json',key='jkg_json_filename')
        self.jkg_schema_filename = cfg.get_value(section='jkg_json',key='jkg_schema_filename')

        # Load the nodes array from the JKG JSON into Polars dataframes.
        self._load_jkg_json(max_nodes=max_nodes, max_rels=max_rels)








