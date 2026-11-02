"""
jkg_json.py
Class that manages the JKG JSON.
"""

import os
import gc
import ijson
import pandas as pd
from tqdm import tqdm
from typing import Any
import pyarrow as pa

# Centralized logging object
from .ubkg_logging import ubkgLogging
# Application configuration object
from .ubkg_config import ubkgConfigParser
# Progress bar wrapper for reads of large JSON files
from .progressfile import ProgressFile
# tqdm-based timer
from .ubkg_timer import UbkgTimer
# file i/o
from .ubkg_extract import ubkgExtract
# streaming Parquet writer
from .parquet_bucket_writer import ParquetBucketWriter

class Jkgjson:

    def _flatten_jkg_json_object_to_row_dict(self, object_type: str, item: dict)->dict:
        """
        Flattens a nested JSON object from the JKG JSON file to a Python dictionary.
        :param object_type: The type of object to flatten.
        :param item: an item dict read from ijson
        :return: a dict corresponding to the flattened JSON object
        """

        """
        Flatten according to the JKG JSON schema.
        Identify keys that do not match the schema.
        """

        if object_type == "nodes.item":

            row = {"labels": item.get("labels", [])}
            # Superset of property fields for all node objects.
            node_property_keys = [
                "id", # all nodes
                "sab", # all nodes
                "name", # Source
                "source", # Source (URL)
                "description", # Source
                "source_version", # Source
                "srl", # Source
                "ttyl", # Source
                "node_label", # Node_Label
                "def", # Node_Label
                "rel_label", # Rel_Label
                "pref_term" # Concept
                ]
            properties = item.get("properties", {})
            for k,v in properties.items():
                if k in node_property_keys:
                    row[f"properties_{k}"] = v
                else:
                    print(row)
                    raise Exception(f"Unknown node property: {k}")

        elif object_type == "rels.item":
            row = {
                    "label": item.get("label"),
                    "start_id": item.get("start", {}).get("properties", {}).get("id"),
                    "end_id": item.get("end", {}).get("properties", {}).get("id")
                   }
            # Property fields for CODE rel objects.
            coderel_property_keys = [
                "sab",  # all rels
                "codeid",  # CODE rels
                "tty",  # CODE rels
                "def",  # CODE rels,
                "attribute",
                "valuestr",
                "value",
                "upperbound",
                "lowerbound",
                "unit"
            ]
            # Property keys for non-CODE rel objects
            non_coderel_property_keys = [
                "sab", # all rels
                "attribute",
                "valuestr",
                "value",
                "upperbound",
                "lowerbound",
                "unit"
                ]
            properties = item.get("properties", {})

            """
            The JKG JSON schema specifies an optional set of keys for 
                attribute
                valuestr
                value
                upperbound
                lowerbound
                unit
            However, because Parquet is a columnar format, the columns must be specified before
            writing to file.
            """
            has_attribute_key = False
            has_valuestr_key = False
            has_value_key = False
            has_upperbound_key = False
            has_lowerbound_key = False
            has_unit_key = False

            if item.get("label") == "CODE":
                rel_property_keys = coderel_property_keys
                rel_property_name = "CODE rel"
            else:
                rel_property_keys = non_coderel_property_keys
                rel_property_name = "non-CODE rel"

            for k,v in properties.items():
                if k in rel_property_keys:
                    row[f"properties_{k}"] = v
                else:
                    raise Exception(f"Unknown {rel_property_name} property: {k}")

                if k == "attribute":
                    has_attribute_key = True
                if k == "valuestr":
                    has_valuestr_key = True
                if k == "value":
                    has_value_key = True
                if k == "upperbound":
                    has_upperbound_key = True
                if k == "lowerbound":
                    has_lowerbound_key = True
                if k == "unit":
                    has_unit_key = True
            if not has_attribute_key:
                row["properties_attribute"] = ""
            if not has_valuestr_key:
                row["properties_valuestr"] = ""
            # value, upperbound, and lowerbound are numeric.
            if not has_value_key:
                row["properties_value"] = None
            if not has_upperbound_key:
                row["properties_upperbound"] = None
            if not has_lowerbound_key:
                row["properties_lowerbound"] = None
            if not has_unit_key:
                row["properties_unit"] = ""
        else:
            raise Exception(f"Unknown JSON object type : {object_type}")

        return row

    def _define_schemas(self):

        # Defines Parquet schemas per file type.

        # Source
        SOURCE_SCHEMA = pa.schema([
            ("labels", pa.list_(pa.string())),
            ("properties_id", pa.string()),
            ("properties_name", pa.string()),
            ("properties_description", pa.string()),
            ("properties_sab", pa.string()),
            ("properties_source_version", pa.string()),
            ("properties_source", pa.string()),
            ("properties_srl", pa.string()),
            ("properties_ttyl", pa.list_(pa.string()))
        ])

        # Node_Label
        NODE_LABEL_SCHEMA = pa.schema([
            ("labels", pa.list_(pa.string())),
            ("properties_id", pa.string()),
            ("properties_def", pa.string()),
            ("properties_node_label", pa.string()),
            ("properties_sab", pa.string()),
        ])

        # Rel_Label
        REL_LABEL_SCHEMA = pa.schema([
            ("labels", pa.list_(pa.string())),
            ("properties_id", pa.string()),
            ("properties_def", pa.string()),
            ("properties_rel_label", pa.string()),
            ("properties_sab", pa.string())
        ])

        # Concept
        CONCEPT_SCHEMA = pa.schema([
            ("labels", pa.list_(pa.string())),
            ("properties_id", pa.string()),
            ("properties_pref_term", pa.string()),
            ("properties_sab", pa.string())
        ])

        # Term
        TERM_SCHEMA = pa.schema([
            ("labels", pa.list_(pa.string())),
            ("properties_id", pa.string())
        ])

        # coderel
        CODEREL_SCHEMA = pa.schema([
            ("label", pa.string()),
            ("start_id", pa.string()),
            ("end_id", pa.string()),
            ("properties_sab", pa.string()),
            ("properties_def", pa.string()),
            ("properties_tty", pa.string()),
            ("properties_codeid", pa.string()),
            ("properties_attribute", pa.string()),
            ("properties_valuestr", pa.string()),
            ("properties_value", pa.float64()),
            ("properties_lowerbound", pa.float64()),
            ("properties_upperbound", pa.float64()),
            ("properties_unit", pa.string())
        ])

        # rel
        REL_SCHEMA = pa.schema([
            ("label", pa.string()),
            ("start_id", pa.string()),
            ("end_id", pa.string()),
            ("properties_sab", pa.string()),
            ("properties_attribute", pa.string()),
            ("properties_valuestr", pa.string()),
            ("properties_value", pa.float64()),
            ("properties_lowerbound", pa.float64()),
            ("properties_upperbound", pa.float64()),
            ("properties_unit", pa.string())
        ])

        self.SCHEMAS = {
            "Source": SOURCE_SCHEMA,
            "Node_Label": NODE_LABEL_SCHEMA,
            "Rel_Label": REL_LABEL_SCHEMA,
            "Concept": CONCEPT_SCHEMA,
            "Term": TERM_SCHEMA,
            "coderel": CODEREL_SCHEMA,
            "rel": REL_SCHEMA
        }

    def _define_paths(self):

        # Paths to Parquet files.
        source_nodes_path = os.path.join(self.jkg_json_dir, 'source_nodes.parquet')
        node_label_nodes_path = os.path.join(self.jkg_json_dir, 'node_label_nodes.parquet')
        rel_label_nodes_path = os.path.join(self.jkg_json_dir, 'rel_label_nodes.parquet')
        concept_nodes_path = os.path.join(self.jkg_json_dir, 'concept_nodes.parquet')
        term_nodes_path = os.path.join(self.jkg_json_dir, 'term_nodes.parquet')
        rel_rows_path = os.path.join(self.jkg_json_dir, 'rels.parquet')
        code_rel_rows_path = os.path.join(self.jkg_json_dir, 'coderels.parquet')

        self.paths = {
            "Source": source_nodes_path,
            "Node_Label": node_label_nodes_path,
            "Rel_Label": rel_label_nodes_path,
            "Concept": concept_nodes_path,
            "Term": term_nodes_path,
            "rel": rel_rows_path,
            "coderel": code_rel_rows_path
        }

    def _load_jkg_json(self):

        """
        Does the following:

        1. Streams the JKG JSON file, separating objects into Parquet files:

           - Nodes
             - Source nodes
             - Node_Label nodes
             - Rel_Label nodes
             - Concept nodes
             - Term nodes
           - Relationships (rels)
             - "coderels" or concept-term (code) relationships
             - other rels (concept-concept relationships)

           The structure of streamed rows depends on the content:
           - nodes: one row per node, with 'labels' as a list column and properties flattened as columns
           - rels: one row per relationship, with start/end/properties flattened as columns

        """

        self.log.print_and_logger_info('*** STREAMING JKG JSON FILE to a set of Parquet files ***')

        # Build the full path to JKG JSON file.
        jkg_json_full = os.path.join(self.jkg_json_dir, self.jkg_json_filename)
        if not (os.path.exists(jkg_json_full)):
            self.log.print_and_logger_error(f"JKG JSON file {jkg_json_full} not found.")
            exit(1)

        # Get the file size of JKG JSON for tqdm.
        self.file_size = os.path.getsize(jkg_json_full)

        """
        SET UP STREAMING PARQUET BUCKET WRITERS.
        Each JSON object streamed from the JKG JSON will be converted to a flattened row and added to a
        "bucket" that corresponds to the object's type. The bucket's writer will write batches of row
        content to a Parquet file for the object type.
        """

        node_buckets = {
            "Source": ParquetBucketWriter(path=self.paths["Source"],schema=self.SCHEMAS["Source"]),
            "Node_Label": ParquetBucketWriter(path=self.paths["Node_Label"],schema=self.SCHEMAS["Node_Label"]),
            "Rel_Label": ParquetBucketWriter(path=self.paths["Rel_Label"],schema=self.SCHEMAS["Rel_Label"]),
            "Concept": ParquetBucketWriter(path=self.paths["Concept"],schema=self.SCHEMAS["Concept"]),
            "Term": ParquetBucketWriter(path=self.paths["Term"],schema=self.SCHEMAS["Term"]),
        }
        rel_bucket = ParquetBucketWriter(path=self.paths["rel"],schema=self.SCHEMAS["rel"])
        code_rel_bucket = ParquetBucketWriter(path=self.paths["coderel"],schema=self.SCHEMAS["coderel"])

        # Initialize object type counts.
        counts = {k: 0 for k in node_buckets}
        rel_count = 0
        code_rel_count = 0

        # Stream through the JKG JSON file.
        try:

            with open(jkg_json_full, "rb") as f:

                # tqdm progress indicator
                with tqdm(desc=f"Reading from {self.jkg_json_filename}",
                          total=self.file_size,
                          unit="B", unit_scale=True, unit_divisor=1024) as pbar:

                    # Wrap the ijson streaming read with a custom tqdm progress bar.
                    pf = ProgressFile(f, pbar)

                    # Iterate parse events (i.e., read the file). Use ijson.ObjectBuilder to reconstruct each item.
                    builder = None

                    for prefix, event, value in ijson.parse(pf):

                        """
                        If the parse event corresponds to the start of a node or rel object,
                        instantiate an ijson ObjectBuilder object.
                        """
                        if prefix in ["nodes.item","rels.item"] and event == "start_map":
                            builder = ijson.ObjectBuilder()

                        if builder is not None:
                            # Process the node or rel information.
                            builder.event(event, value)

                            """
                            If this is the end parsing event of the current item, 
                            flatten the object into a row.
                            """
                            object_type = prefix

                            if event == "end_map" and object_type in ("nodes.item", "rels.item"):

                                row = self._flatten_jkg_json_object_to_row_dict(item=builder.value, object_type=object_type)

                                if object_type == "nodes.item":
                                    # Split node objects by type.

                                    labels = row.get("labels", [])
                                    matched = False

                                    for label_key in ("Source", "Node_Label", "Rel_Label", "Concept", "Term"):
                                        if label_key in labels:
                                            """
                                            Add the row to the buffer of the appropriate bucket writer. 
                                            The bucket writer will manage writes to the corresponding Parquet file..
                                            """
                                            node_buckets[label_key].add(row)
                                            counts[label_key] += 1
                                            matched = True
                                            break
                                    if not matched:
                                        raise Exception(f"Unknown node type: {labels}")

                                else:

                                    """
                                    Split coderels from non-CODE rels.
                                    Add row to the buffer of the appropriate bucket writer. 
                                    """

                                    if row.get("label") == "CODE":
                                        # Exclude MTH:NOCODE codes from the UMLS.
                                        if row.get("properties_codeid") != "MTH:NOCODE":
                                            code_rel_bucket.add(row)
                                            code_rel_count += 1
                                    else:

                                        rel_bucket.add(row)
                                        rel_count += 1

                                builder = None
        finally:
            # Close open writer buckets so that bucket writers correctly write Parquet footers in files..
            for bucket in node_buckets.values():
                bucket.close()
            rel_bucket.close()
            code_rel_bucket.close()

        # Report counts.
        self.log.print_and_logger_info(f'*** JKG JSON LOAD SUMMARY:')
        self.log.print_and_logger_info('* NODE OBJECTS')
        self.log.print_and_logger_info(f"---- Source nodes: {counts['Source']:,}")
        self.log.print_and_logger_info(f"---- Node_Label nodes: {counts['Node_Label']:,}")
        self.log.print_and_logger_info(f"---- Rel_Label nodes: {counts['Rel_Label']:,}")
        self.log.print_and_logger_info(f"---- Concept nodes: {counts['Concept']:,}")
        self.log.print_and_logger_info(f"---- Term nodes: {counts['Term']:,}")
        self.log.print_and_logger_info('* REL OBJECTS')
        self.log.print_and_logger_info(f"---- non-CODE rels: {rel_count:,}")
        self.log.print_and_logger_info(f"---- CODE rels: {code_rel_count:,}")

        """
        Remove duplicate term nodes by:
        1. Re-reading from the Parquet file of term node information into a Pandas DataFrame.
        2. Dropping duplicates.
        3. Writing the de-duplicated Pandas DataFrame back to the Parquet file.
        """
        self.log.print_and_logger_info("Dropping duplicates from terms Parquet file.")
        dfterms = self.load_dataframe(filename='term_nodes')
        dfterms = dfterms.drop_duplicates('properties_id')
        self._export_unload_dataframe(dfexport=dfterms,filename='term_nodes')


    def _export_unload_dataframe(self, dfexport:pd.DataFrame, filename: str):
        """
        Exports a DataFrame to a Parquet file and frees memory.

        The use case is a DataFrame containing a subset of nodes from the JKG JSON file.
        :param dfexport: DataFrame
        :param filename: name of the export file, without extension
        """

        outfile = os.path.join(self.jkg_json_dir, filename + '.parquet')
        self.uextract.to_parquet_with_progress_bar(df=dfexport, outpath=outfile)

        self._unload_item(item_to_unload=dfexport)

    def load_dataframe(self,filename:str)->pd.DataFrame:
        """
        Imports a DataFrame from a temporary Parquet file.
        :param filename: name of the temporary file, without extension
        :return: DataFrame
        """

        infile = os.path.join(self.jkg_json_dir, filename + '.parquet')
        self.log.print_and_logger_info(f'Loading DataFrame from {infile}')
        return self.uextract.read_parquet_with_progress_bar(path=infile)

    def _unload_item(self, item_to_unload:Any):
        """
        Explicitly unloads an object from memory.
        :param item_to_unload: object to be unloaded

        """

        self.log.print_and_logger_info(f'Unloading {type(item_to_unload)}.')
        if type(item_to_unload) is list:
            item_to_unload.clear()
        if type(item_to_unload) is pd.DataFrame:
            item_to_unload = None

        gc.collect()

    def __init__(self, log: ubkgLogging, cfg: ubkgConfigParser) -> None:
        self.log = log
        self.cfg = cfg

        # Get path to the JKG JSON.
        self.jkg_json_dir = cfg.get_value(section='jkg_json',key='jkg_json_dir')
        self.jkg_json_filename = cfg.get_value(section='jkg_json',key='jkg_json_filename')
        self.jkg_schema_filename = cfg.get_value(section='jkg_json',key='jkg_schema_filename')

        # For exporting and importing DataFrames of subsets of the JKG JSON.
        self.uextract = ubkgExtract(ulog=self.log)

        # Define Parquet schemas and output paths
        self._define_schemas()
        self._define_paths()

        # Stream the JKG JSON file and separate the different types of nodes and rels into Parquet files.
        self._load_jkg_json()










