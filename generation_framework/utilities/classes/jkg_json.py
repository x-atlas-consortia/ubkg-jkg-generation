"""
jkg_json.py
Class that manages the JKG JSON.
"""

import os
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

            # --- NODES ---
            # Each node: { "labels": [...], "properties": { "id": ..., "sab": ..., ... } }
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

            # --- RELS ---
            # Each rel: { "label": ..., "start": { "properties": { "id": ... } },
            #             "end":   { "properties": { "id": ... } },
            #             "properties": { "sab": ..., ... } }
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
        Loads the JKG JSON file into two Pandas dataframes.
        :param max_nodes: maximum number of nodes to load. None loads all nodes.
        :param max_rels: maximum number of rels to load. None loads all rels.
        Sets both:
        - self.jkg_nodes: one row per node, with 'labels' as a list column and properties flattened as columns
        - self.jkg_rels: one row per relationship, with start/end/properties flattened as columns
        """

        # Treat 0 or None as "load all"
        max_nodes = None if not max_nodes else max_nodes
        max_rels = None if not max_rels else max_rels

        if max_nodes is not None and max_rels is not None:
            self.log.print_and_logger_info(f'Loading only {max_nodes} nodes and {max_rels} rels from {self.jkg_json_filename}.')

        # Build the full path to JKG JSON file.
        jkg_json_full = os.path.join(self.jkg_json_dir, self.jkg_json_filename)
        # Get the file size of JKG JSON for tqdm
        file_size = os.path.getsize(jkg_json_full)

        node_rows = []
        rel_rows = []

        with open(jkg_json_full, "rb") as f:

            with tqdm(desc=f"Reading from {self.jkg_json_filename}",
                      total=file_size,
                      unit="B", unit_scale=True, unit_divisor=1024) as pbar:

                # Wrap the ijson read with a progress bar.
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
                                row = {
                                    "labels": item.get("labels", []),
                                    "properties_id": item.get("properties", {}).get("id"),
                                    "properties_name": item.get("properties", {}).get("name"),
                                    "properties_description": item.get("properties", {}).get("description"),
                                    "properties_sab": item.get("properties", {}).get("sab"),
                                    "properties_source": item.get("properties", {}).get("source"),
                                    "properties_source_version": item.get("properties", {}).get("source_version"),
                                    "properties_node_label": item.get("properties", {}).get("node_label"),
                                    "properties_rel_label": item.get("properties", {}).get("rel_label"),
                                    "properties_def": item.get("properties", {}).get("def"),
                                    "properties_pref_term": item.get("properties", {}).get("pref_term"),
                                }

                                node_rows.append(row)

                            elif prefix == "rels.item":
                                row = {
                                    "label": item.get("label"),
                                    "start_id": item.get("start", {}).get("properties", {}).get("id"),
                                    "end_id": item.get("end", {}).get("properties", {}).get("id"),
                                    "properties_sab": item.get("properties", {}).get("sab"),
                                    "properties_def": item.get("properties", {}).get("def"),
                                    "properties_tty": item.get("properties", {}).get("tty"),
                                    "properties_codeid": item.get("properties", {}).get("codeid"),
                                }
                                row.update(item.get("properties", {}))
                                rel_rows.append(row)

                            builder = None

            utimer = UbkgTimer(display_msg="Loading JKG JSON nodes")
            #self.jkg_nodes = pl.DataFrame(node_rows, infer_schema_length=len(node_rows))
            self.jkg_nodes = pd.DataFrame(node_rows)
            utimer.stop()

            utimer = UbkgTimer(display_msg="Loading JKG JSON rels")
            #self.jkg_rels = pl.DataFrame(rel_rows, infer_schema_length=len(rel_rows))
            self.jkg_rels=pd.DataFrame(rel_rows)
            utimer.stop()

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







