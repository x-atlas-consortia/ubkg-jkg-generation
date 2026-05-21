"""
jkg_import.py
Sabjkgimport class that adds new nodes and rels objects to an existing
JKG JSON.

The source of new nodes and rels objects is a set of files in
JKG Edde/Node (JkGEN) format.

"""

from typing import Any
import os
import ast
import gc
import pandas as pd
from pandas.core.computation.ops import isnumeric
from tqdm import tqdm

# logging object
from .ubkg_logging import ubkgLogging
# configuration
from .ubkg_config import ubkgConfigParser
# JKG JSON handling
from .jkg_json import Jkgjson
# JKG edge and node file handling
from .jkg_edgenode import Jkgedgenode
# JKG JSON writer
from .json_writer import JsonWriter
# UBKG code and relationship standardizer
from .ubkg_standardizer import ubkgStandardizer
# spinner timer
from .ubkg_timer import UbkgTimer
# sources.json handling
from .ubkg_sources import ubkgSources

class Sabjkgimport:

    def __init__(self, sab:str, ulog:ubkgLogging, cfg: ubkgConfigParser, repo_root: str):

        # Transfer properties.
        # SAB
        self.sab = sab
        # Logging object
        self.ulog = ulog
        # Config object
        self.cfg = cfg
        # Absolute file reference
        self.repo_root = repo_root

        # Instantiate UBKG code and relationship standardizer object
        self.ustand = ubkgStandardizer(ulog=ulog, repo_root=repo_root)
        # Instantiate UBKG-JKG sources manager object
        self.usource = ubkgSources(ulog=ulog, cfg=cfg, repo_root=repo_root)

        # Whether to overwrite the JKG JSON as part of ingestion.
        self.ingest_overwrite = cfg.get_value(section='jkg_json', key='ingest_overwrite').lower() == 'true'

        # Initialize lists of output objects.
        self._initialize_lists()

        # Load the nodes and rels arrays from the original JKG JSON.
        self.jkgjson = Jkgjson(log=ulog, cfg=cfg)

        # Verify that the SAB does not already exist in the JKG JSON.
        if not self.jkgjson.source_nodes.empty:
            if self.sab.upper() in self.jkgjson.source_nodes['properties_sab'].values:
                self.ulog.print_and_logger_error(f"The SAB '{self.sab.upper()}' already exists in the JKG JSON.")
                exit(1)

        # Input/Output directory for JKG JSON.
        self.jkgjson_dir = os.path.join(self.repo_root,
                                        self.cfg.get_value(section='jkg_json',
                                                           key='jkg_json_dir'))

        # Load the JKGEN edge and node files for the new SAB.
        self._load_jkgen()

        # Ordered list of tuples that stores counts of nodes before and after ingestion
        self.node_counts = []

        # Initialize the output file.
        # The output file has the same name as the input JKG JSON,
        # so this will overwrite the input file.
        self._start_new_jkgjson()

        # BUILD AND WRITE THE NODES ARRAY.

        # Start the array.
        self.jkgjson_writer.start_list(keyname='nodes')

        # Add new node objects from JKGEN to the existing
        # nodes array from the JKG JSON.
        self._build_and_write_nodes_array()

        # End the array.
        self.jkgjson_writer.end_list()

        # Add delimiters between the nodes and rels arrays.
        self.jkgjson_writer.write_comma()
        self.jkgjson_writer.write_line_feed()

        # BUILD AND WRITE THE RELS ARRAY.

        # Start the array.
        self.jkgjson_writer.start_list(keyname='rels')

        # Add new rel objects from JKGEN to the existing
        # rel array from the JKG JSON.
        self._build_and_write_rels_array()

        # End the array.
        self.jkgjson_writer.end_list()

        # End the JKG JSON.
        self.jkgjson_writer.end_json()

        # Print out comparisons of node counts.
        self._report_node_counts()
        self._report_node_counts(to_file=True)


    def _update_node_counts(self, node_type: str, state: str, count: int):
        """
        Updates the list of node count tuples
        :param node_type: type of node
        :param state: "before", "after", or "updated"
        :param count: number of nodes of type node_type at state
        """
        if state not in ['before', 'after', 'updated']:
            raise ValueError(f'Invalid state for node count: {state}')


        self.node_counts.append((node_type, state, count))

    def _report_node_counts(self, to_file: bool=False):
        """
        Prints out before and after counts of nodes.
        :param to_file: whether to print the report to file

        Assumes that the list of tuples in self.node_counts is ordered
        to match the workflow--e.g.,
        [('Source', 'before', 107), ('Source', 'after', 108)...]
        """

        if not to_file:
            print('')

        if to_file:
            outfilepath = os.path.join(self.sab_jkg_dir,'node_counts.tsv')



        self.ulog.print_and_logger_info("*** COMPARISONS OF NODE COUNTS ***")
        # Group the list of tuples into a dict.
        data = {}
        for node_type, state, count in self.node_counts:
            data.setdefault(node_type, {})[state] = count

        # Print out the dict in a table.
        w_type = 20
        w_before = 20
        w_after = 20
        w_updated = 20
        w_border = 90

        if to_file:
            with open(outfilepath, 'w') as outfile:
                outfile.write(f'type\tbefore\tafter\tupdated\n')
        else:
            self.ulog.print_and_logger_info(f"{'type':<{w_type}} {'before':>{w_before}} {'after':>{w_after}} {'updated':>{w_updated}}")
            self.ulog.print_and_logger_info("-" * w_border)

        for k, v in data.items():
            before = v.get("before", 0)
            updated = v.get("updated", 0)
            after = v.get("after", 0)
            if k == 'non-CODE rels':
                if to_file:
                    with open(outfilepath, 'a') as outfile:
                        outfile.write(f'{k}\t{before}\t{after}\t{updated}\n')
                else:
                    self.ulog.print_and_logger_info(
                        f"{k:<{w_type}} {before:>{w_before},} {after:>{w_after},} {updated:>{w_updated},}")
            else:
                if to_file:
                    with open(outfilepath, 'a') as outfile:
                        outfile.write(f'{k}\t{before}\t{after}\tn/a\n')
                else:
                    self.ulog.print_and_logger_info(
                        f"{k:<{w_type}} {before:>{w_before},} {after:>{w_after},} {"n/a":>{w_updated}}")

        self.ulog.print_and_logger_info("-" * w_border)

    def _initialize_lists(self):
        """
        Initialize lists of objects for new JKG JSON nodes and rels
        arrays, by type:
        1. for nodes:
           a. sources
           b. concepts
           c. terms
        2. for rels:
           a. coderels
           b. rels
        The objects in each list will be added below the corresponding
        part of the list in the JKG JSON--e.g., the new sources will be
        added after the original sources in the nodes array.
        (The ingested SAB will add no new node_label or rel_label types
        of node objects.)
        """

        # Lists related to nodes
        self.list_new_jkg_json_source_nodes = []
        self.list_new_jkg_json_concept_nodes = []
        self.list_new_jkg_json_term_nodes = []
        # Lists related to rels
        self.list_new_jkg_json_coderels = []
        self.list_new_jkg_json_rels = []

    def _unload_item(self, item_to_unload:Any):
        """
        Explicitly unloads an object from memory.
        :param item_to_unload: object to be unloaded

        """
        if type(item_to_unload) is list:
            item_to_unload.clear()
        if type(item_to_unload) is pd.DataFrame:
            item_to_unload = None

        gc.collect()

    def _load_jkgen(self):

        """
        Loads the JKG edge and node files for a SAB.

        """
        self.sab_jkg_dir = os.path.join(self.repo_root,
                                        self.cfg.get_value(section="directories",
                                                           key="sab_jkg_dir"),
                                        self.sab)
        self.jkgen = Jkgedgenode(log=self.ulog,
                                 cfg=self.cfg,
                                 sab=self.sab,
                                 filedir=self.sab_jkg_dir)

        # Standardize the values in the "node_synonyms" column per
        # the codeid regex pattern in JKG Schema.

        self.jkgen.nodes['node_synonyms'] = self.ustand.standardize_synonyms(x=self.jkgen.nodes['node_synonyms'])

    def _start_new_jkgjson(self):

        """
        Initializes the new JKG JSON file by means of a
        JsonWriter object.

        """
        self.ulog.print_and_logger_info("*** REBUILDING JKG JSON FILE ***")
        if self.ingest_overwrite:
            outpath = os.path.join(self.jkgjson.jkg_json_dir, self.jkgjson.jkg_json_filename)
        else:
            outpath = os.path.join(self.jkgjson.jkg_json_dir, 'new_jkg.json')

        # Use a JsonWriter object to build the new JKGJSON.
        self.jkgjson_writer = JsonWriter(outpath=outpath)
        # Start the JSON.
        self.jkgjson_writer.start_json()

    def _build_and_write_nodes_array(self):
        """
        Does the following:
        1. Converts information from the JKGEN nodes file
           into lists of new node objects of types:
           a. Source
           b. Concept
           c. Term
        2. Combines the lists of new node objects with
           corresponding lists of original node objects from
           the JKGJSON
        3. Writes lists in order to the nodes array of the
           new JKGJSON file.

        """

        # Build and write the updated list of Source nodes.
        self._build_and_write_source_nodes()

        # Node_Labels
        self.ulog.print_and_logger_info('* NODE_LABEL NODES')
        self._build_and_write_node_label_nodes()

        # Rel_Labels
        self.ulog.print_and_logger_info('* REL_LABEL NODES')
        self._build_and_write_rel_label_nodes()

        """
        GET CUIS FOR JKGEN NODES.
        
        Apply the equivalence class algorithm to
        identify the CUIs to which to assign new nodes.
        """
        self.jkgen.nodes['cuis'] = self._get_cuis_for_nodes()

        # Write the results of the algorithm to the JKGEN directory.
        cuifile = os.path.join(self.sab_jkg_dir, 'node_cuis.csv')
        self.jkgen.nodes.to_csv(cuifile, index=False)

        self.ulog.print_and_logger_info('* CONCEPT NODES')
        # Concept nodes.
        self._build_and_write_concept_nodes()

        self.ulog.print_and_logger_info('* TERM NODES')
        # Term nodes.
        self._build_and_write_term_nodes()

    def _unflatten_dataframe_and_write_list(self, df_flat: pd.DataFrame, progress_display: str="", unload_frame:bool=False):

        """
        Does the following:
        1. Converts a DataFrame of "flattened" information in JKG JSON format to
           a list of "unflattened" (nested) objects.
        2. Writes the unflattened list to output.
        3. Optionally unloads the input DataFrame from memory.

        :param df_flat: DataFrame of "flattened" information in JKG JSON format
        :param progress_display: name used for the tqdm progress bar
        :param unload_frame: whether to unload the DataFrame.

        """
        list_unflat = self._convert_flat_dataframe_to_unflat_list(df_flat=df_flat,
                                                                  progress_display=progress_display)
        self._unload_item(item_to_unload=df_flat)

        self.jkgjson_writer.write_list(list_name=progress_display, list_content=list_unflat)

    def _convert_flat_dataframe_to_unflat_list(self, df_flat: pd.DataFrame, progress_display: str="", unload_frame:bool=False) -> list:
        """
        For purposes of analysis, the nested JSON objects from
        the JKG JSON are converted to DataFrames in which
        nested key/value pairs are "flattened" to fields by means
        of prefixes.

        This function converts a flattened DataFrame into a list
        of "unflattened" dicts, reconstituting to the nested
        structure.

        Optionally unloads the input DataFrame from memory.

        :param df_flat: DataFrame to convert
        :param progress_display: display for progress bar
        :param unload_frame: whether to unload the DataFrame.
        :return: converted list

        """

        # Convert DataFrame to list.
        list_flat = df_flat.to_dict(orient='records')
        if len(list_flat) == 0:
            list_flat = []

        # Convert list of flattened objects to list of "unflattened" (nested) objects.
        return self._unflatten_objects(list_flat_objects=list_flat,
                                       progress_display=progress_display)


    def _build_and_write_source_nodes(self):
        """
        Does the following:
        1. Builds a source node object for the JKGEN SAB.
        2. Appends it to the list of source node objects
           from the JKG JSON.
        3. Writes the combined list to output.

        """

        self.ulog.print_and_logger_info('* SOURCE NODES')
        # Convert the DataFrame of flattened original source nodes
        # to a list of unflattened (nested) objects.
        list_unflat_sources = self._convert_flat_dataframe_to_unflat_list(df_flat=self.jkgjson.source_nodes, progress_display='existing Source nodes (JKG JSON)')
        self._update_node_counts(node_type="Source", state="before", count=len(list_unflat_sources))

        # Build the source node for the SAB.
        # (Although there is only one source, treat as a
        # list with one element for purposes of combination.)
        new_source = self._build_sab_source_node()

        # Add the new source node to the list of nested source nodes.
        list_unflat_sources.extend(new_source)

        # Write the complete nested list to output.
        self._update_node_counts(node_type="Source", state="after", count=len(list_unflat_sources))
        self.jkgjson_writer.write_list(list_name='all Source nodes (JKGJSON + JKGEN)', list_content=list_unflat_sources)

    def _build_sab_source_node(self) -> list[dict]:

        """
        Builds an unflattened (nested) JKG JSON Source node for a non-UMLS SAB.
        Source information for a non-UMLS SAB resides in the
        sources.json file at the root of the repository.

        :return: a list with a single dict that will be combined with
                 the JKG JSON list of source node dicts.

        """
        # Source manager
        usource = ubkgSources(ulog=self.ulog, cfg=self.cfg, repo_root=self.repo_root)

        source_type = usource.get(sab=self.sab, key='source_type')
        source_name = usource.get(sab=self.sab, key='name')
        source_description = self.usource.get(sab=self.sab, key='description')
        source_version = self.usource.get(sab=self.sab, key='version')

        # The "properties_" prefix flattens key/values that are to be
        # nested into a "properties" object.

        dictsource = {
            "labels": ["Source"],
            "properties": {
                "id": f"{self.sab.upper()}:{self.sab.upper()}",
                "name": source_name,
                "description": source_description,
                "sab": f"{self.sab.upper()}",
                "source_version": source_version,
                "ttyl": ["PT","SY"] # always only PT or SY
                }
            }

        # Sources from SABs will have a URL.
        if source_type == "owl":
            dictsource["properties"]["source"] = usource.get(sab=self.sab, key='owl_url')

        return [dictsource]

    def _build_and_write_node_label_nodes(self):

        """
        Does the following:
        1. Unflattens the list of exiting Node_Label node objects from the JKG JSON.
           from the JKG JSON.
        2. Writes the list to output.

        The types of Node_Label types are defined by the JKG Schema.
        New ingestions will not define new Node_Label types.

        """

        write_delimiters = len(self.jkgjson.node_label_nodes) > 0
        if write_delimiters:
            self.jkgjson_writer.write_comma()
            self.jkgjson_writer.write_line_feed()

        self._unflatten_dataframe_and_write_list(df_flat=self.jkgjson.node_label_nodes,
                                                 progress_display='existing Node_Label nodes (JKG JSON)')

        # No net new node labels.
        self._update_node_counts(node_type='Node_Labels', state="before", count=len(self.jkgjson.node_label_nodes))
        self._update_node_counts(node_type='Node_Labels', state="after", count=len(self.jkgjson.node_label_nodes))

        # Unload the Node_Label nodes.
        self._unload_item(item_to_unload=self.jkgjson.node_label_nodes)

    def _build_and_write_rel_label_nodes(self):
        """
        Does the following:
        1. Builds a list of unflattened Rel_Label nodes for Rel_Labels linked
           to predicate labels from the JKGEN edge file that are not already
           in the array of Rel_Label nodes in JKG JSON.
        2. Unflattens the list of exiting Rel_Label node objects from the JKG JSON.
           from the JKG JSON.
        3. Combines the list of new Rel_Label nodes and original Rel_Label nodes.
        3. Writes the combined list to output.

        """

        # Build list of unflattened objects for new Rel_Label nodes.
        list_new_unflat_rel_labels = self._build_new_rel_label_nodes()

        # Convert the DataFrame of flattened original Rel_Label nodes
        # to a list of unflattened (nested) objects.
        list_unflat_rel_labels = self._convert_flat_dataframe_to_unflat_list(df_flat=self.jkgjson.rel_label_nodes,
                                                                           progress_display='existing Rel_Label nodes (JKG JSON)')
        self._update_node_counts(node_type="Rel_Label", state="before", count=len(list_new_unflat_rel_labels))

        # Add the list of new nested concept nodes to the list of original nested concept nodes.
        list_unflat_rel_labels.extend(list_new_unflat_rel_labels)
        self._update_node_counts(node_type="Rel_Label", state="after", count=len(list_unflat_rel_labels))

        self._unload_item(item_to_unload=list_new_unflat_rel_labels)

        write_delimiters = len(list_unflat_rel_labels) > 0
        if write_delimiters:
            self.jkgjson_writer.write_comma()
            self.jkgjson_writer.write_line_feed()

        # Write the complete nested list to output.
        self.jkgjson_writer.write_list(list_name='all Rel_Label nodes (JKG JSON + JKGEN)',
                                       list_content=list_unflat_rel_labels)

        # Unload the Rel_Label nodes DataFrame.
        self._unload_item(item_to_unload=self.jkgjson.rel_label_nodes)

    def _build_new_rel_label_nodes(self) -> list[dict]:
        """
        Builds a list of unflattened Rel_Label objects for predicates
        in the JKGEN edge file that are not in the existing set of
        Rel_Label nodes in JKG JSON.

        To identify new Rel_Label objects, the function compares the
        predicate string from the JKGEN edg against the rel_label
        property of JKG JSON Rel_Label objects. This means that if
        a predicate string from an JKGEN import matches an
        existing Rel_Label object, the predicate will be treated as
        already existing in JKG JSON.

        In other words, there is a risk of lost information for a SAB
        that has an edge that has a meaning different from a similar
        edge that is already in JKG JSON. For example, if "part_of" is
        already in JKG JSON when a SAB is ingested, "part_of" will mean what
        it means in JKG JSON--not necessarily what "part_of" means in the SAB.

        The JKG framework makes a reasonable attempt to use standardized
        relation labels from either UMLS or Relations Ontology. The risk
        of a semantic conflict seems low--e.g., "part_of" is likely to
        mean the same thing across SABs. The risk of semantic conflict
        is outweighed by the benefit of avoiding unncessarily different
        versions of the same relationship--e.g.,
        a "ABC:part_of" from SAB ABC that really means the same as
        the standard "part_of".

        """

        # Compute existing Rel_Label node values once as a set — O(1) lookups
        if self.jkgjson.rel_label_nodes.empty:
            existing_rel_labels = set()
        else:
            existing_rel_labels = set(self.jkgjson.rel_label_nodes['properties_rel_label'])

        # Filter to only new relation labels in a single pass
        df_new = self.jkgen.edges[~self.jkgen.edges['predicate'].isin(existing_rel_labels)].drop_duplicates(subset='predicate')

        # Build the result. Wrap in tqdm.
        sab_upper = self.sab.upper()
        return [
            {
                "labels": ["Rel_Label"],
                "properties": {
                    "id": f"{sab_upper}:{row.predicate}",
                    "def": row.predicate,
                    "rel_label": row.predicate,
                    "sab": sab_upper
                }
            }
            for row in
            tqdm(df_new.itertuples(index=False), total=len(df_new), desc="-- Building new Rel_Label nodes (JKGEN)")
        ]

    def _build_and_write_concept_nodes(self):
        """
        Does the following:
        1. Builds a list of unflattened Concept nodes for new concepts linked
           to node codes from the JKGEN.
        2. Unflattens the list of Concept node objects from the JKG JSON.
           from the JKG JSON.
        3. Combines the list of new Concept nodes and original Concept nodes.
        3. Writes the combined list to output.

        """

        # Build list of unflattened objects for new concept nodes.
        list_new_unflat_concepts = self._build_new_concept_nodes()

        # Convert the DataFrame of flattened original concept nodes
        # to a list of unflattened (nested) objects.
        list_unflat_concepts = self._convert_flat_dataframe_to_unflat_list(df_flat=self.jkgjson.concept_nodes, progress_display='existing Concept nodes (JKG JSON)')
        self._update_node_counts(node_type="Concept", state="before", count=len(list_unflat_concepts))

        # Add the list of new nested concept nodes to the list of original nested concept nodes.
        list_unflat_concepts.extend(list_new_unflat_concepts)
        self._update_node_counts(node_type="Concept", state="after", count=len(list_unflat_concepts))

        self._unload_item(item_to_unload=list_new_unflat_concepts)

        write_delimiters = len(list_unflat_concepts) > 0
        if write_delimiters:
            self.jkgjson_writer.write_comma()
            self.jkgjson_writer.write_line_feed()

        # Write the complete nested list to output.
        self.jkgjson_writer.write_list(list_name='all Concept nodes (JKG JSON + JKGEN)', list_content=list_unflat_concepts)

    def _build_new_concept_nodes(self) -> list[dict]:
        """
        Builds a list of unflattened concept objects for all
        new CUIs to which a node's code links, if not already
        present in coderels.
        """

        """
        1. Explode so each linked CUI gets its own row, keeping node_label aligned.
           
           The equivalence algorithm (get_cuis_for_nodes) will not assign a CUI for 
           a node that was added to the nodes list from the edge file and that already
           has a CUI defined in JKG. Drop references to these nodes.
        """
        df_exploded = (
            self.jkgen.nodes[['cuis', 'node_label']]
            .explode('cuis')
            .rename(columns={'cuis': 'cui'})
        ).dropna() # from edge nodes added to nodes list


        # 2. Compute existing CUIs once as a set — O(1) lookups
        if self.jkgjson.coderels.empty:
            existing_cuis = set()
        else:
            existing_cuis = set(self.jkgjson.coderels['properties_codeid'])

        # 3. Filter to only new CUIs in a single pass
        df_new = df_exploded[~df_exploded['cui'].isin(existing_cuis)]
        
        # Unload exploded DataFrame.
        self._unload_item(item_to_unload=df_exploded)

        # 4. Build the result. Wrap in tqdm.
        sab_upper = self.sab.upper()
        return [
            {
                "labels": ["Concept"],
                "properties": {
                    "id": row.cui,
                    "pref_term": row.node_label,
                    "sab": sab_upper
                }
            }
            for row in tqdm(df_new.itertuples(index=False), total=len(df_new), desc="-- Building new concept nodes (JKG JSON)")
        ]

    def _build_and_write_term_nodes(self):
        """
        Does the following:
        1. Builds a list of unflattened Term nodes for new terms for nodes
           from the JKGEN.
        2. Appends the list to the list of flattened Term node objects
           from the JKG JSON.
        3. Unflattens the combined list.
        3. Writes the combined list to output.

        """

        # Convert the DataFrame of flattened original term nodes
        # to a list of unflattened (nested) objects.
        list_unflat_terms = self._convert_flat_dataframe_to_unflat_list(df_flat=self.jkgjson.term_nodes,
                                                                        progress_display='existing Term nodes (JKG JSON)')
        self._update_node_counts(node_type="Term", state="before", count=len(list_unflat_terms))

        # Build list of unflattened objects for new term nodes.
        list_new_unflat_terms = self._build_new_term_nodes()

        # Unload the DataFrame of term nodes.
        self._unload_item(item_to_unload=self.jkgjson.term_nodes)

        # Add the list of new nested term nodes to the list of original nested term nodes.
        list_unflat_terms.extend(list_new_unflat_terms)
        
        # Unload the list of new term nodes.
        self._unload_item(item_to_unload=list_new_unflat_terms)

        write_delimiters = len(list_unflat_terms) > 0
        if write_delimiters:
            self.jkgjson_writer.write_comma()
            self.jkgjson_writer.write_line_feed()

        # Write the complete nested list to output.
        self._update_node_counts(node_type="Term", state="after", count=len(list_unflat_terms))
        self.jkgjson_writer.write_list(list_name='all Term nodes (JKG JSON + JKGEN)', list_content=list_unflat_terms)

    def _build_new_term_nodes(self) -> list[dict]:
        """
        Builds Term objects for the JKG JSON nodes array from a JKGEN node.
        :return: list of Term objects (dicts) for:
                 - the node
                 - the node's synonyms
        """
        listret = []

        """
            Keys with the "properties_" index will eventually
            be "unflattened" and moved to a nested dict.
            
            First, build Terms for the node's preferred term, corresponding to the 
            node_label field.
        """

        # Drop duplicates.
        df_nodes= self.jkgen.nodes.drop_duplicates(subset='node_label')
        # Filter to terms that are not already in JKGJSON.
        if not self.jkgjson.term_nodes.empty:
            df_nodes = df_nodes.merge(self.jkgjson.term_nodes,
                                                   how='left',
                                                   left_on='node_label',
                                                   right_on='properties_id')

            df_nodes = df_nodes[df_nodes['properties_id'].isnull()]

        listret.extend(
            [
                {
                    "labels": ["Term"],
                    "properties": {
                        "id": row.node_label
                    }
                }
                for row in tqdm(df_nodes.itertuples(), total=len(df_nodes), desc="-- Building new Term nodes for node preferred terms (JKGEN)")
            ]
        )

        # Build Terms for the node's synonyms.
        # Explode on synonyms list.
        df_exploded_syn = (
            self.jkgen.nodes[['node_id', 'node_synonyms']]
            .explode('node_synonyms')
            .rename(columns={'node_synonyms': 'node_synonym'})
            .reset_index(drop=True)
        ).dropna(subset=['node_synonym'])

        # Only add terms if there are synonyms.
        df_exploded_syn = df_exploded_syn[df_exploded_syn['node_synonym'] != '']

        # Drop duplicates.
        df_exploded_syn = df_exploded_syn.drop_duplicates(subset='node_synonym')

        # Only add terms for synonyms that are not already in either
        # new preferred terms or existing terms.
        if not self.jkgen.nodes.empty:
            df_exploded_syn = df_exploded_syn.merge(self.jkgen.nodes,
                                            how='left',
                                            left_on='node_synonym',
                                            right_on='node_label')
            df_exploded_syn = df_exploded_syn[df_exploded_syn['node_label'].isnull()]

        if not df_exploded_syn.empty:
            df_exploded_syn = df_exploded_syn.drop_duplicates(subset='node_synonym')

            if not self.jkgjson.term_nodes.empty:
                df_exploded_syn = (df_exploded_syn.merge(self.jkgjson.term_nodes,
                               how='left',
                               left_on='node_synonym',
                               right_on='properties_id')
                                   .rename(columns={'properties_id_y': 'properties_id_'}))

                if not df_exploded_syn.empty:
                    df_exploded_syn = df_exploded_syn[df_exploded_syn['properties_id'].isnull()]

        listret.extend(
            [
                {
                    "labels": ["Term"],
                    "properties": {
                        "id": row.node_synonym
                    }
                }
                for row in
                tqdm(df_exploded_syn.itertuples(), total=len(df_exploded_syn), desc="-- Building new Term nodes for node synonyms (JKGEN)")
            ]
        )

        return listret

    def _build_and_write_rels_array(self):
        """
        Does the following:
        1. Converts information from the JKGEN edges file
           into lists of new rels objects of types:
           a. "coderels"--concept to term (node)
           b. rels--concept-to-concept
        2. Combines the lists of new rel objects with
           corresponding lists of original rel objects from
           the JKGJSON
        3. Writes lists in order to the rels array of the
           new JKGJSON file:
           a. existing non-CODE rels
           b. new non-CODE rels
           c. existing CODE rels
           d. new CODE rels

        """

        # Count of CODE rels from JKG JSON.
        self._update_node_counts(node_type="CODE rels", state="before", count=len(self.jkgjson.coderels))

        """
        Build list of new coderels for the nodes and their synonyms.
        The list of coderels is also used in analysis of non-CODE rels and 
        so is retained in memory longer than other lists.
        """

        self.list_new_coderels = self._build_new_coderels()

        # Count of CODE rels after.
        all_coderel_count = len(self.list_new_coderels) + len(self.jkgjson.coderels)
        self._update_node_counts(node_type="CODE rels", state="after", count=all_coderel_count)

        """
        Use the new coderels to update any existing rels from
        prior ingestions for which the CUIs were updated in 
        the current ingestion.
        """
        self._update_node_cuis_in_rels()

        """
        WRITE EXISTING RELS TO OUTPUT.
        """
        # Keep track of the number of exiting rels.
        num_existing_rels = len(self.jkgjson.rels)

        self._unflatten_dataframe_and_write_list(df_flat=self.jkgjson.rels, progress_display='existing non-CODE rels')

        # Unload DataFrame of existing rels.
        self._unload_item(item_to_unload=self.jkgjson.rels)

        """
        BUILD AND WRITE NEW RELS TO OUTPUT.
        
        Build new rels, using both edges from JKGEN and
        new coderels.
        """

        list_new_rels = self._build_new_non_coderels()
        self._update_node_counts(node_type="non-CODE rels", state="after", count=len(self.jkgjson.rels) + len(list_new_rels))

        """
        Determine whether to add delimiters between 
        list of existing rels and list of new rels.
        """

        num_new_rels = len(list_new_rels)
        if num_new_rels > 0 and num_existing_rels > 0:
            self.jkgjson_writer.write_comma()
            self.jkgjson_writer.write_line_feed()

        progress_display = 'new non-CODE rels'

        # Convert list of flattened new rels objects to a list of "unflattened" (nested) new rels objects.
        list_unflat_new_rels = self._unflatten_objects(list_flat_objects=list_new_rels, progress_display=progress_display)
        self._unload_item(item_to_unload=list_new_rels)

        self.jkgjson_writer.write_list(list_name=progress_display, list_content=list_unflat_new_rels)
        self._unload_item(item_to_unload=list_unflat_new_rels)

        """
        WRITE EXISTING CODERELS TO OUTPUT.
        """

        """
        Determine whether to add delimiters between the 
        new rels list and the existing coderels list.
        """
        num_existing_coderels = len(self.jkgjson.coderels)
        if num_existing_coderels > 0 and num_new_rels > 0:
            self.jkgjson_writer.write_comma()
            self.jkgjson_writer.write_line_feed()

        self._unflatten_dataframe_and_write_list(df_flat=self.jkgjson.coderels, progress_display='existing CODE rels')

        # Unload DataFrame of existing coderels.
        self._unload_item(item_to_unload=self.jkgjson.coderels)

        """
        WRITE NEW CODERELS TO OUTPUT.
        The new coderels were built earlier in the workflow.
        """

        """
        Determine whether to add delimiters between
        the list of existing coderels and list of new coderels.
        """

        num_new_coderels = len(self.list_new_coderels)
        if num_new_coderels > 0 and num_existing_coderels > 0:
            self.jkgjson_writer.write_comma()
            self.jkgjson_writer.write_line_feed()

        progress_display = 'new CODE rels'
        # Convert list of flattened new coderel objects to a list of "unflattened" (nested) new coderel objects.

        list_unflat_new_coderels=self._unflatten_objects(list_flat_objects=self.list_new_coderels, progress_display=progress_display)
        # Unload list of new coderels.
        self._unload_item(item_to_unload=self.list_new_coderels)

        self.jkgjson_writer.write_list(list_name=progress_display, list_content=list_unflat_new_coderels)
        self._unload_item(item_to_unload=list_unflat_new_coderels)


    def _build_new_coderels(self)-> list[dict]:
        """
        Builds a list of new coderel objects for the JKG JSON rels array from JKGEN nodes.

        :return: list of coderel objects (dicts) for:
             - the node
             - the node's synonyms
        """

        list_new_coderels = []

        """
        Explode the DataFrame of JKGEN nodes on the CUIs that
        were assigned by the equivalence class algorithm.
        """

        df_nodes_exploded_on_cuis = (
            self.jkgen.nodes
            .explode('cuis')
            .rename(columns={'cuis': 'cui'})
            .reset_index(drop=True)
        )

        """
        Identify coderels that do not already exist in the JKG JSON.
        These correspond to new concepts introduced by the JKGEN node file.
        
        """
        if self.jkgjson.coderels.empty:
            # Defensive. It is unlikely that the original JKG JSON would not have any concepts.
            df_new_coderels = df_nodes_exploded_on_cuis
        else:
            df_new_coderels = (
                df_nodes_exploded_on_cuis.merge(
                    self.jkgjson.coderels[['properties_codeid', 'start_id']],
                    how='left',
                    left_on=['node_id', 'cui'],
                    right_on=['properties_codeid', 'start_id'],
                    indicator=True
                )
                .query('_merge == "left_only"')
                .drop(columns=['properties_codeid', 'start_id', '_merge'])
            )
        """
        Drop any coderels without CUIs.
        (This handles cases in which the node identifier is a UMLS CUI 
        or a node that already has a CUI in JKG. The equivalence
        algorithm assigns these cases no new CUI.)
        """
        df_new_coderels = df_new_coderels[~df_new_coderels['cui'].isnull()]

        """
            The nodes DataFrame is flattened.
             
            In addition, the nodes file can include optional columns that 
            correspond to custom node properties.
            
            Add any values from optional columns with a "properties_" prefix to indicate that they will
            be in the eventual nested properties object.
            e.g., if the nodes file has a column "X", the properties 
            dict will contain a key "X".

        """

        # Identify the set of optional columns.
        excluded_cols = self._get_node_base_cols()
        optional_cols = [col for col in df_new_coderels.columns if col not in excluded_cols]

        """
        Build the flattened coderels that correspond to the PT term type for
        new concepts. These will link a Concept node object
        to a Term node object corresponding to the node label, with
        properties that define the node's code.
        
        Use the packing operator to add the custom properties.
        
        """

        list_new_coderels.extend(
            [
                {
                    "label": "CODE",
                    "start_id": row.cui,
                    "end_id": row.node_label,
                    "properties_sab": self.sab,
                    "properties_def": row.node_definition,
                    "properties_codeid": row.node_id,
                    "properties_tty": "PT",
                    # ** unpacks a dict built per-row from the optional columns
                    **{f"properties_{col}": getattr(row, col) for col in optional_cols}
                }
                for row in tqdm(df_new_coderels.itertuples(),
                                total=len(df_nodes_exploded_on_cuis),
                                desc="-- Building Coderel objects for new concepts (term type = PT)")

            ]
        )

        # Unload the exploded DataFrame.
        self._unload_item(item_to_unload=df_nodes_exploded_on_cuis)

        """
        Build the flattened coderels that correspond to the SY term type for
        new concepts. These will link a Concept node object
        to a Term node object corresponding to each synonym, with
        properties that define the node's code.
        
        """

        # Explode again, this time on synonyms list.
        df_exploded_on_cuis_synonyms = (
            df_new_coderels
            .explode('node_synonyms')
            .rename(columns={'node_synonyms': 'node_synonym'})
            .reset_index(drop=True)
        )
        # Filter to codes with synonyms.
        df_exploded_on_cuis_synonyms = df_exploded_on_cuis_synonyms[df_exploded_on_cuis_synonyms['node_synonym']!='']

        # Terms of type SY do not get the definition.
        list_new_coderels.extend(
            [
                {
                    "label": "CODE",
                    "start_id": row.cui,
                    "end_id": row.node_synonym,
                    "properties_sab": self.sab,
                    "properties_def": "",
                    "properties_codeid": row.node_id,
                    "properties_tty": "SY",
                    # ** unpacks a dict built per-row from the optional columns
                    **{f"properties_{col}": getattr(row, col) for col in optional_cols}
                }
                for row in tqdm(df_exploded_on_cuis_synonyms.itertuples(),
                                total=len(df_exploded_on_cuis_synonyms),
                                desc="-- Building Coderel objects for new concepts (term type = SY)")

            ]
        )
        return list_new_coderels

    def _get_node_base_cols(self)-> set:
        """
        Identifies base node property columns for JKGEN nodes.

        The nodes file can have a variable number of columns after the
        node_dbxrefs column.
        These columns correspond to custom node properties.
        Examples of node properties are:
        - value
        - lowerbound
        - upperbound
        - unit

        """

        # Define the set of base columns that will be excluded
        # from the list of optional property values.
        return {'node_id',
                     'node_label',
                     'node_definition',
                     'node_synonyms',
                     'node_dbxrefs',
                     'cui',
                     # Exclude coderel merge artifacts:
                     'start_id',
                     'end_id',
                     'label',
                     'properties_sab',
                     'properties_def',
                     'properties_tty',
                     'properties_codeid',
                     'labels',
                     'properties_id'
                     }


    def _update_node_cuis_in_rels(self):
        """
        Updates existing rels from previous ingestions
        that involve nodes for which CUI-code links were updated
        by the current ingestion.

        A code from a vocabulary can be specified as a node
        into JKG in more than one ingestion.

        It is often the case that one SAB's node file will refer to
        a code that does not have a CUI in the existing coderels data.
        The equivalence class algorithm will create a new concept
        for this code with a default CUI in format "<code> CUI".
        This CUI is used to build rels objects for concept-concept
        relationships involving the code as defined in the edge file.

        If the code's SAB is subsequently ingested, the node file
        for the SAB may specify cross-references for the code that
        result in new CUI assignments. It is then necessary to
        replace all existing rels that involve the code's CUI with
        new rels that use the cross-referenced CUIs.

        For example:
        1. SAB1 specifies
           - node in node file with node_id codeSAB2:code2
           - edge in edge file with SAB1:code1 -[rel1]-> SAB2:code2
           Because SAB2 was not ingested prior to SAB1,
           SAB2:code2 is linked to a concept with
           CUI= "SAB2:code2 CUI". The rel uses SAB2:code2 CUI.
        2. SAB2 specifies node with
           - node_id SAB2:code2
           - node_dbxrefs that links SAB2:code2 to the CUIs
             CUI1 and CUI2.

           The rel involving CUI "SAB2:code2 CUI" must be
           replaced with two rels in which "SAB2:code2 CUI" is replaced
           with one of the two new cross-referenced CUIs.

        """

        if self.jkgjson.coderels.empty:
            self._update_node_counts(node_type="non-CODE rels", state="updated", count=0)
            return


        # Obtain count of rels before updates.
        self._update_node_counts(node_type="non-CODE rels", state="before", count=len(self.jkgjson.rels))
        list_updated = 0

        # Set of rel field names that are not for custom node properties.
        base_cols = {
            'start_id',
            'end_id',
            'label',
            'old_cui',
            'new_cui',
            'properties_codeid'
        }

        # Convert lists of new coderels to a dataframe for merging.
        df_new_coderels = pd.DataFrame(self.list_new_coderels)

        """
        GET CHANGES TO CUIS IN OLD CODERELS
        
        Obtain CUIs identified in prior ingestions in JKG JSON
        that are also in nodes in the current ingestion (via JKGEN).
        """
        df_changed_cuis = ((df_new_coderels.merge(self.jkgjson.coderels,
                                              how='inner',
                                              on='properties_codeid')
                           .rename(columns={'start_id_x': 'new_cui',
                                            'start_id_y': 'old_cui'}))
                           .drop_duplicates(subset=['old_cui','new_cui','properties_codeid']))

        # Unload the DataFrame of new coderels.
        self._unload_item(item_to_unload=df_new_coderels)

        df_changed_cuis = df_changed_cuis[['old_cui','new_cui','properties_codeid']]

        # Filter to those CUIs were minted from the node id.
        df_changed_cuis = df_changed_cuis[df_changed_cuis['old_cui']==df_changed_cuis['properties_codeid'] + ' CUI']
        gc.collect()

        """
        For some data sources, no nodes have cross-references.
        Examples include Data Distillery datasets.
        
        """
        if df_changed_cuis.empty:
            self._update_node_counts(node_type="non-CODE rels", state="updated", count=0)
            return

        """
            REPLACE RELS WITH CHANGED END CUIS. 
        """

        # Get rels from the JKG JSON for which the end CUI changed.
        df_rels_changed_cuis_end = (self.jkgjson.rels.merge(df_changed_cuis,
                                                             how='inner',
                                                             left_on='end_id',
                                                             right_on='old_cui'))

        log_updated = len(df_rels_changed_cuis_end)

        # Identify the custom node properties.
        custom_prop_cols = [c for c in df_rels_changed_cuis_end.columns if c not in base_cols]

        """
        For each rel for which the end CUI changed,
        add new rels for each new end CUI.
        """
        list_new_rels_end = []
        list_new_rels_end.extend(
            [
                {
                    "label": row.label,
                    "start_id": row.start_id,
                    "end_id": row.new_cui, # new CUI
                    **{c: getattr(row, c) for c in custom_prop_cols}
                }
                for row in tqdm(df_rels_changed_cuis_end.itertuples(), total=len(df_rels_changed_cuis_end),
                                desc="-- Updating rels with changed end CUIs")
            ]
        )
        # Unload the DataFrame of rels with changed end CUIs.
        self._unload_item(item_to_unload=df_rels_changed_cuis_end)

        # Convert the flattened list of new rels to a DataFrame for concatenating.
        df_new_rels_end = pd.DataFrame(list_new_rels_end)
        # Unload the flattened list of new rels.
        self._unload_item(item_to_unload=list_new_rels_end)

        # Add the DataFrame of new rels to the DataFrame of original rels.
        self.jkgjson.rels = pd.concat([self.jkgjson.rels, df_new_rels_end])
        # Unlaod the DataFrame of new rels.
        self._unload_item(item_to_unload=df_new_rels_end)

        # Delete the original rels that use the old CUIs.
        self.jkgjson.rels = self.jkgjson.rels[~self.jkgjson.rels['end_id'].isin(df_rels_changed_cuis_end['old_cui'])]

        gc.collect()

        """
            REPLACE RELS WITH CHANGED START CUIS. 
            
            Note: some rels may have changed both the start and end 
            CUIs. 
        """

        # Get rels from the JKG JSON for which the start CUI changed.
        df_rels_changed_cuis_start = (self.jkgjson.rels.merge(df_changed_cuis,
                                                            how='inner',
                                                            left_on='start_id',
                                                            right_on='old_cui'))

        # Update count of updated rels.
        # If both the start and end CUIs were updated, then a rel will be counted more than once.
        log_updated = len(df_rels_changed_cuis_end) + len(df_rels_changed_cuis_start)
        self._update_node_counts(node_type="non-CODE rels", state="updated", count=99)

        # Unload the DataFrame of changes in CUIs.
        self._unload_item(item_to_unload=df_changed_cuis)

        custom_prop_cols = [c for c in df_rels_changed_cuis_start.columns if c not in base_cols]

        # For each rel for which the start CUI changed,
        # add new rels for each new start CUI.

        list_new_rels_start = []
        list_new_rels_start.extend(
            [
                {
                    "label": row.label,
                    "start_id": row.start_id,
                    "end_id": row.new_cui,  # new CUI
                    **{c: getattr(row, c) for c in custom_prop_cols}
                }
                for row in tqdm(df_rels_changed_cuis_start.itertuples(), total=len(df_rels_changed_cuis_end),
                                desc="-- Updating rels with changed start CUIs")
            ]
        )

        # Unload the DataFrame of rels with changed end CUIs.
        self._unload_item(item_to_unload=df_rels_changed_cuis_start)

        # Convert the flattened list of new rels to a DataFrame for concatenating.
        df_new_rels_start = pd.DataFrame(list_new_rels_start)
        # Unload the flattened list of new rels.
        self._unload_item(item_to_unload=list_new_rels_start)

        # Add the DataFrame of new rels to the DataFrame of original rels.
        self.jkgjson.rels = pd.concat([self.jkgjson.rels, df_new_rels_start])
        # Unload the DataFrame of new rels.
        self._unload_item(item_to_unload=df_new_rels_start)

        # Delete the original rels with the old CUIs.
        self.jkgjson.rels = self.jkgjson.rels[~self.jkgjson.rels['start_id'].isin(df_rels_changed_cuis_start['old_cui'])]
        gc.collect()

    def _parse_cui_list(self, val):
        """
        Parse a cui value.
        :param val: representation of a CUI that may be:
                    - a list string
                    - an empty list string
                    - NaN

        """

        if pd.isna(val) if not isinstance(val, list) else False:
            return []
        if isinstance(val, list):
            return val
        try:
            parsed = ast.literal_eval(val)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, SyntaxError):
            return []


    def _mint_new_cui(self, code:Any) -> str:
        """
        Mints a new CUI based on a code.
        :param code: code, presumably in format SAB:code
        """
        if type(code) == str:
            return code + ' CUI'
        elif type(code) == pd.Series:
            return code.iloc[0] + ' CUI'
        else:
            raise TypeError(f"Minting new CUI: Unknown type for code {code}")

    def _get_cuis_for_nodes(self) -> pd.Series:
        """

        Implements the UBKG-JKG equivalence class algorithm.

        Identifies the CUIs for the concepts to which to link
        JKGEN nodes, based on a ranked evaluation of the cross-references
        (dbxrefs) for each JKGEN node.

        ---
        The dbxref column of each row in the DataFrame
        is a pipe-delimited list of cross-references.

        Examples:
        node_id           node_dbxrefs
        UBERON:0001748    emapa:35663|fma:55566|umls:c0927176|ma:0002676|ncit:c33265
        UBERON:0005030    fma:59772
        MP:0011739        cl:0002084

        Cross-references can be of the following types:
        1. A UMLS CUI (e.g., umls:c0927176). The dbxref is a "direct equivalence"
           --i.e., the node is explicitly mapped to a UMLS CUI.
        2. Codes linked to CUIs that are in the JKG JSON. The dbxref is a
           "transitive equivalence".
           Transitive codes are of two types:
           a. A code from a UMLS vocabulary that has a dbxref to a UMLS CUI.
              In the case of UBERON:0005030, FMA is a UMLS vocabulary,
              and fma:55566 has a UMLS CUI.
           b. A code from a non-UMLS vocabulary that was ingested into JKG
              prior to the current ingestion, with a non-UMLS CUI.
              This is known as an "other code". MP:0011739 is an example: its
              dbxref is to CL:0002084. When CL is ingested prior to MP,
              it has a CUI ("CL:0002084 CUI") in JKGJSON.

        The available CUIs for a code from a vocabulary (SAB)
        are ranked in order of preference:
        1. the first direct UMLS CUI
        2. the first UMLS CUI for a transitive code
        3. the first other CUI for a transitive code
        4. a CUI minted from the code
        """

        # This is a block operation with no hooks for tqdm,
        # so start a spinner.
        utimer = UbkgTimer(display_msg="* IDENTIFYING CUIS FOR NODES")

        direct_umls_map = {}
        other_umls_map = {}
        other_non_umls_map = {}
        node_cui_map = {}

        """
        1. Explode to one row per dbxref.
        
        In other words, transform the DataFrame rows  
        
            from original:
            node_id node_dbxrefs
            node1   SAB1:Code1|SAB2:Code2
            
            to:
            node_id node_dbxrefs
            node1   [SAB1:Code1, SAB2:Code2]
            (done by JKGEdgeNode object)
        
            then to:
            node_id node_dbxrefs
            node1   SAB1:Code1
            node1   SAB2:Code2
        """

        df_nodes = self.jkgen.nodes.copy()

        df_exploded = self.jkgen.nodes[['node_id', 'node_dbxrefs']].explode('node_dbxrefs').reset_index(drop=True)

        """
        2. Standardize dbxref codes, grouping by SAB. 
           (The standardization of a code depends on the code SAB.)
           a. Extract SABs from dbxref codes.
           b. Convert each dbxref into a Series to standardize the code.
           c. Group standardized codes by SAB.
           d. Collect standardized codes into lists.
        """
        # 2a.
        df_exploded['sab'] = df_exploded['node_dbxrefs'].str.split(':').str[0]
        # 2b-d.
        if df_exploded.empty:
            # Default empty Series.
            df_exploded['node_dbxrefs'] = pd.Series(dtype=str)
        else:
            df_exploded['node_dbxrefs'] = pd.concat([
                pd.Series(
                    self.ustand.standardize_code(x=group['node_dbxrefs'], sab=sab_val).tolist(),
                    index=group.index
                )
                for sab_val, group in df_exploded.groupby('sab')
            ])

        """
        3. Identify direct UMLS CUIs--dbxrefs that start with 'umls:'.
           a. Filter to only those dbxrefs that start with UMLS.
           b. Create a "map" of dbxref to lists of UMLS CUIs. 
              Group by node_id and collect UMLS CUIs into lists.
        """
        # 3a.
        # In dbxrefs, UMLS CUIs are in lowercase.
        df_direct_umls = df_exploded.copy()
        df_direct_umls = df_direct_umls[df_direct_umls['node_dbxrefs'].str.lower().str.startswith('umls:')]
        df_direct_umls['node_dbxrefs'] = df_direct_umls['node_dbxrefs'].apply(lambda x: str(x).upper())

        # 3b.
        # The map is a dict in format
        # {'UBERON:0001794':[UMLS:c1512783]...}

        direct_umls_map = (
            df_direct_umls.groupby('node_id', sort=False)['node_dbxrefs']
            .apply(lambda x: x.dropna().unique().tolist())
            .to_dict()
        )

        """
        4. Identify "other CUIs"--dbxrefs with codes that have CUIs in coderels.
           a. Merge against coderels.
           b. Group by node_id and collect CUIs into lists.
           c. Distinguish CUIs by whether they are from the UMLS or a non-UMLS SAB.
           
        """
        if self.jkgjson.coderels.empty:
            """
            Defensive, as it is unlikely that the JKG JSON will have 
            no concept-code links. 
            It can happen if the source JKG JSON is actually one of 
            the "batched" files that contain only a nodes array.
            """
            df_other = df_exploded
            other_umls_map = {}
            other_non_umls_map = {}
        else:
            # Get the other CUIs for each dbxref from coderels.
            # The merge renames the node label, so restore the header.
            df_other = (df_exploded.merge(self.jkgjson.coderels,
                                          how='left',
                                          left_on='node_dbxrefs',
                                          right_on='properties_codeid')
                        .rename(columns={'node_label_x': 'node_label'}))

            """
            4b. Split other CUIs into UMLS and non-UMLS
            The filter on dbxrefs not including UMLS prevents
            direct UMLS dbxrefs from being included again here.
            """

            df_other_umls = df_other[
                df_other['start_id'].str.startswith('UMLS', na=False) &
                ~df_other['node_dbxrefs'].str.upper().str.startswith('UMLS', na=False)
                ]
            df_other_non_umls = df_other[~df_other['start_id'].str.startswith('UMLS', na=False)]

            """
            Create "maps" of dbxrefs to lists of CUIs. 
            These maps are dicts with a dbxref for a key
            and a value that is a list of CUIs--e.g.,
            {'UBERON:0000006': ['UMLS:C0022131'],...}
            """

            other_umls_map = (
                df_other_umls.groupby('node_id')['start_id']
                .apply(lambda x: x.dropna().unique().tolist())
                .to_dict()
            )
            other_non_umls_map = (
                df_other_non_umls.groupby('node_id')['start_id']
                .apply(lambda x: x.dropna().unique().tolist())
                .to_dict()
            )

            # Unload analysis DataFrames that are no longer needed.
            self._unload_item(item_to_unload=df_exploded)
            self._unload_item(item_to_unload=df_direct_umls)
            self._unload_item(item_to_unload=df_other)
            self._unload_item(item_to_unload=df_other_umls)
            self._unload_item(item_to_unload=df_other_non_umls)

            """
            Identify any nodes that already have a CUI.            
            """
            df_node_cui = (df_nodes.merge(self.jkgjson.coderels,
                                          how='left',
                                          left_on='node_id',
                                          right_on='properties_codeid')
                        .rename(columns={'node_label_x': 'node_label'}))

            node_cui_map = (
                df_node_cui.groupby('node_id')['start_id']
                .apply(lambda x: x.dropna().unique().tolist())
                .to_dict()
            )

            self._unload_item(item_to_unload=df_node_cui)

            """
            Identify CUIs for the node_id. Select the first CUI from lists 
            in order of:
            1. direct UMLS CUIs
            2. other UMLS CUIs
            3. other non-UMLS CUIs
            
            If no CUI identified, then check whether the node itself is 
            linked to a CUI. If not, then mint a new CUI.
                
            """

        utimer.stop()

        return df_nodes['node_id'].map(
        lambda node_id: self._get_cuis_from_maps(
            node_id=node_id,
            direct_umls_map=direct_umls_map,
            other_umls_map=other_umls_map,
            other_non_umls_map=other_non_umls_map,
            node_cui_map=node_cui_map
        )
    )

    def _get_cuis_from_maps(self, node_id: str,
                                direct_umls_map: dict,
                                other_umls_map: dict,
                                other_non_umls_map: dict,
                                node_cui_map: dict) -> list[str]:

        """
        Selects all available CUIs for a node's code, using map objects
        built in the _get_cuis_for_nodes function.

        :param node_id: code for a node
        :param direct_umls_map: map of nodes to direct UMLS CUIs
        :param other_umls_map: map of nodes to other UMLS CUIs
        :param other_non_umls_map: map of nodes to other non-UMLS CUIs
        :param node_cui_map: map of existing CUIs for the node

        Each map has keys that are codes from the JKGEN node file, with
        values that are lists of CUIs--e.g.,
        # {'UBERON:0001794':[UMLS:c1512783]...}

        :return: a list of CUI strings

        Rule: only consider cross-references for a node that shares
        SAB with the ingestion SAB--i.e., cross-references from the
        source that maintains the code for the node.

        """

        # Check the maps of code to CUIs.
        direct_umls_cuis = direct_umls_map.get(node_id, [])
        other_umls_cuis = other_umls_map.get(node_id, [])
        other_non_umls_cuis = other_non_umls_map.get(node_id, [])
        node_cuis = node_cui_map.get(node_id, [])

        all_cuis = []

        # Only assign cross-references if the ingestion SAB
        # corresponds to the node SAB.
        sab_node = node_id.split(':')[0].upper()
        if sab_node == self.sab:

            if direct_umls_cuis:
                all_cuis = all_cuis + direct_umls_cuis

            if other_umls_cuis:
                all_cuis = all_cuis +  other_umls_cuis

            if other_non_umls_cuis:
                all_cuis = all_cuis + other_non_umls_cuis

        """
        Defaults:
        If there was no CUI obtained from dbxrefs, check whether
        the node itself either is a UMLS CUI or has a link to a CUI.
        If not, then mint a new CUI.
        
        This will result in some nodes having no CUI. 
        Coderels for these nodes will not be created.
        """
        if not (direct_umls_cuis
                or other_umls_cuis
                or other_non_umls_cuis
                or ('UMLS:' in node_id)
                or node_cuis):
            all_cuis =  [self._mint_new_cui(node_id)]

        # Get unique list of CUIs in original order, by rank.
        return list(dict.fromkeys(all_cuis))

    def _map_restored_custom_col_names(self, dfrels: pd.DataFrame,custom_prop_cols:list):

        """
        Used to restore custom property column names for which a _x suffix was attacjed
        due to naming collisions.
        :param dfrels: Frame of JKGEN rels information
        :param custom_prop_cols: list of custom property column names
        :return: dict to be used in renaming.
        """

        rename_map = {}
        for col in custom_prop_cols:
            if col not in dfrels.columns and f'{col}_x' in dfrels.columns:
                rename_map[f'{col}_x'] = col
        return rename_map

    def _build_new_non_coderels(self) -> list[dict]:
        """
        Translates the edges in a JKGEN edge file to a list of
        new concept-concept relationships for the
        rels array of the JKG JSON.

        """

        utimer = UbkgTimer(display_msg='Building new non-CODE rels.')

        # Convert list of new coderels to a DataFrame to take
        # advantage of Pandas DataFrame merging.
        dfnewcoderels = pd.DataFrame(self.list_new_coderels)

        # Drop duplicates from merging.(Coderels map cuis to term types.)
        # Remove columns that are irrelevant to CUI identification.
        dfnewcoderels = dfnewcoderels.drop_duplicates(subset=['start_id','properties_codeid'])[['start_id','properties_codeid']]

        """
        CUSTOM EDGE PROPERTIES
        The edge file has a variable number of columns that
        represent edge properties. Include these as flattened
        members of a "properties" object. 

        For each optional column, there will be a corresponding
        "properties_" field. This field will eventually be 
        "unflattened and added to a properties dict in an node object.

        """
        # Set of column names that are not for custom properties.
        base_cols = {
            # Correspond to standard rel keys
            'start_cui',
            'end_cui',
            'predicate',
            'subject',
            'object',
            # merge artifacts
            'properties_sab',
            'properties_def',
            'properties_codeid',
            'properties_tty'
        }
        # Custom property columns.
        custom_prop_cols = [c for c in self.jkgen.edges.columns if c not in base_cols]

        """
        IDENTIFY SUBJECT CUIS
        
        Merge edges against new node coderels to obtain
        CUIs for JKGEN edge subjects.
        
        Drop rels for which the subject node has no CUI.
        (This is defensive. Subject nodes that are not defined
        in the node file are added explicitly to the node 
        DataFrame.)
        """
        self.jkgen.edges = (((self.jkgen.edges.merge(
            dfnewcoderels,
            how ='left',
            left_on ='subject',
            right_on ='properties_codeid')
        .rename(columns = {
            'start_id': 'start_cui'
            }
            ))
        .dropna(subset=['start_cui'])))

        # Restore names of custom property columns that got a _x suffix due to naming collisions.
        rename_map = self._map_restored_custom_col_names(dfrels=self.jkgen.edges, custom_prop_cols=custom_prop_cols)
        if rename_map:
            self.jkgen.edges = self.jkgen.edges.rename(columns=rename_map)

        """
        IDENTIFY OBJECT CUIS
        
        Merge edges against new node coderels to obtain
        CUIs for JKGEN edge objects.
        Drop rels for which the object node has no CUI.
        (This is defensive. Object nodes that are not defined
        in the node file are added explicitly to the node 
        DataFrame.)
        """

        self.jkgen.edges = (((self.jkgen.edges.merge(
            dfnewcoderels,
            how='left',
            left_on='object',
            right_on='properties_codeid')
        .rename(columns={
            'start_id': 'end_cui'
        }
        )).dropna(subset=['end_cui'])))

        # Fix any custom property columns that got a _x suffix due to naming collisions.
        rename_map = self._map_restored_custom_col_names(dfrels=self.jkgen.edges, custom_prop_cols=custom_prop_cols)
        if rename_map:
            self.jkgen.edges = self.jkgen.edges.rename(columns=rename_map)

        # Unload the DataFrame of new code rels used for merges.
        self._unload_item(item_to_unload=dfnewcoderels)

        # Vectorized build, using packing operator.
        # Note that the key for node objects is "label", not "labels".
        return self.jkgen.edges.apply(lambda row: {
            "label": row['predicate'],
            "start_id": row['start_cui'],
            "end_id": row['end_cui'],
            "properties_sab": self.sab,
            **{f"properties_{c}": row[c] for c in custom_prop_cols}
        }, axis=1).tolist()


    def _unflatten_objects(self, progress_display: str = "", list_flat_objects: list = "") -> list:

        """
        Converts a list of flattened JKG JSON objects into a
        "unflattened", or nested, JKG JSON array.

        :param list_flat_objects: list of flattened JKG JSON objects
        :param progress_display: name of list for progress bar
        :return: JKG JSON array

        list_flat_objects can contain a variety of elements.
        Each element is a dict. If the dict key contains a prefix,
        it should go into a nested object--e.g.,
        properties_tty = > {"properties": {"tty":...}}

        Because list_flat_objects may be very large, but must
        be handled in memory, this function contains some
        adjustments to address CPU.
        """

        out = []

        # Reduce tqdm update frequency to address "stuttering" in terminal output.
        for flat in tqdm(list_flat_objects, mininterval=0.5, miniters=100, desc=f"-- Unflattening {progress_display}"):
            unflat = self._unflatten_object(flat_object=flat)
            out.append(unflat)

        return out


    def _unflatten_object(self, flat_object: dict={}) -> dict:

        """
        Converts a single JKG JSON object into a
        "unflattened", or nested item suitable for writing to the JKG JSON.

        :param flat_objects: flattened JKG JSON object
        :return: JKG JSON nested object

        flat_object can contain a variety of elements.
        If the dict key contains a prefix,
        it should go into a nested object--e.g.,
        properties_tty = > {"properties": {"tty":...}}
        """

        unflat = {}

        properties = None
        start_props = None
        end_props = None

        for k, v in flat_object.items():
            # Lazily create nested objects, which should
            # (maybe?) reduce garbage collection.

            if k.startswith("properties_"):
                if properties is None:
                    properties = {}
                # Coerce srl to integer.
                # UMLS and NDC have srl=''; others will have a float.
                if k=="properties_srl" and v != '':
                    vret = int(v)
                else:
                    vret = v
                properties[k[11:]] = vret  # len("properties_") == 11

            elif k.startswith("start_"):
                if start_props is None:
                    start_props = {}
                start_props[k[6:]] = v  # len("start_") == 6

            elif k.startswith("end_"):
                if end_props is None:
                    end_props = {}
                end_props[k[4:]] = v  # len("end_") == 4
            else:
                unflat[k] = v

            if properties:
                unflat["properties"] = properties
            if start_props:
                unflat["start"] = {"properties": start_props}
            if end_props:
                unflat["end"] = {"properties": end_props}

        return unflat

