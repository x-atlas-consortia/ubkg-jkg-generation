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
import json
import pandas as pd
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

        self.sab = sab
        # Logging object
        self.ulog = ulog
        # Config object
        self.cfg = cfg
        # Absolute file reference
        self.repo_root = repo_root
        # UBKG code and relationship standardizer object
        self.ustand = ubkgStandardizer(ulog=ulog, repo_root=repo_root)
        # UBKG-JKG sources manager
        self.usource = ubkgSources(ulog=ulog, cfg=cfg, repo_root=repo_root)

        # Load the nodes and rels arrays from the JKG JSON.
        # The read flattens the arrays into DataFrames.
        self.jkgjson = Jkgjson(log=ulog, cfg=cfg)

        # Get a subset of "coderel" rel objects from the flattened rels array of the JKG JSON.
        if self.jkgjson.rels.empty:
            self.jkg_json_coderels = pd.DataFrame()
        else:
            self.jkg_json_coderels = self.jkgjson.rels[self.jkgjson.rels['label'] == 'CODE']

        """
        Get source node objects from the flattened nodes array of the JKG JSON.
        This requires some Pandas finagling.
        Source nodes have a 'labels' column that is a string list--e.g.,
        "labels: ["Source"]
        So:
        1. Cast as string (required for some mysterious reason)
        2. Apply ast's literal_eval to convert to a list of strings.
        3. Check the first value. 
        """
        if self.jkgjson.nodes.empty:

            """
            Defensive, as it is unlikely that the full JKG JSON will
            have no nodes. It is possible if the JKG JSON file is 
            one of the "batched" JKG JSON files that contains just rels.
            """
            self.jkg_json_sources = pd.DataFrame()
            self.jkg_json_not_sources = pd.DataFrame()

        else:

            utimer = UbkgTimer(display_msg="Separating Source node objects")

            # Parse labels once and cache the result.
            parsed_labels = self.jkgjson.nodes['labels'].astype(str).apply(ast.literal_eval).str[0]

            self.jkg_json_sources = self.jkgjson.nodes[parsed_labels == "Source"]
            self.jkg_json_not_sources = self.jkgjson.nodes[parsed_labels != "Source"]

            utimer.stop()

        # Load the JKGEN edge and node files.
        self.jkgen_dir = os.path.join(repo_root, cfg.get_value(section="directories", key="sab_jkg_dir"), sab)
        self.jkgjson_dir = os.path.join(repo_root, cfg.get_value(section="jkg_json", key="jkg_json_dir"))
        self.jkgen = Jkgedgenode(log=ulog, cfg=cfg, sab=sab, filedir=self.jkgen_dir)


        self.sab_jkg_dir = os.path.join(repo_root, cfg.get_value(section="directories", key="sab_jkg_dir"), sab)

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
        self.new_jkg_json_node_sources = []
        self.new_jkg_json_node_concepts = []
        self.new_jkg_json_node_terms = []
        # Lists related to rels
        self.new_jkg_json_coderels = []
        self.new_jkg_json_rels = []

        # Build lists of components to be added from JKGEN
        # to the JKG JSON nodes array, separated by type.
        self._build_jkgjson_for_jkgen_nodes()

        # Build a list of components to be added from JKGEN
        # to the new JKG JSON rels array.
        self._build_jkgjson_for_jkgen_edges()

        # Add new nodes and rels objects to the JKG JSON and write to output.
        self._build_new_jkgjson()

    def _build_sab_source_node(self) -> dict:

        """
        Builds a JKG JSON Source node for a non-UMLS SAB.
        Source information for a non-UMLS SAB resides in the
        sources.json file at the root of the repository.

        """
        # Source manager
        usource = ubkgSources(ulog=self.ulog, cfg=self.cfg, repo_root=self.repo_root)

        source_type = usource.get(sab=self.sab, key='source_type')
        source_name = usource.get(sab=self.sab, key='name')
        source_description = self.usource.get(sab=self.sab, key='description')
        source_version = self.usource.get(sab=self.sab, key='version')

        dictsource = {
            "labels": ["Source"],
            "properties":
                {"id": f"{self.sab.upper()}:{self.sab.upper()}",
                 "name": source_name,
                 "description": source_description,
                 "sab": f"{self.sab.upper()}",
                 "source_version": source_version}
        }
        if source_type == "owl":
            dictsource["properties"]["source"] = usource.get(sab=self.sab, key='owl_url')

        return dictsource

    def _build_new_concept_nodes(self, dfjkgen_nodes: pd.DataFrame) -> list[dict]:
        """
        Builds concept objects for all new CUIs to which a node's code links,
        if not already present in coderels.
        :param dfjkgen_nodes: DataFrame of JKGEN node data
        """

        # 1. Explode so each linked CUI gets its own row, keeping node_label aligned
        df_exploded = (
            dfjkgen_nodes[['cuis', 'node_label']]
            .explode('cuis')
            .rename(columns={'cuis': 'cui'})
        )

        # 2. Compute existing CUIs once as a set — O(1) lookups
        existing_cuis = set(self.jkg_json_coderels['properties_codeid'])

        # 3. Filter to only new CUIs in a single pass
        df_new = df_exploded[~df_exploded['cui'].isin(existing_cuis)]

        # 4. Build the result. Wrap in tqdm.
        sab_upper = self.sab.upper()
        return [
            {
                "labels": ["Concept"],
                "properties_id": row.cui,
                "properties_pref_term": row.node_label,
                "properties_sab": sab_upper,
            }
            for row in tqdm(df_new.itertuples(index=False), total=len(df_new), desc="Building concept nodes")
        ]

    def _build_new_term_nodes(self, dfjkgen_nodes: pd.DataFrame) -> list[dict]:
        """
        Builds Term objects for the JKG JSON nodes array from a JKGEN node.
        :param dfjkgen_nodes: DataFrame of JKG JSON nodes data
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
        listret.extend(
            [
                {
                    "labels": ["Term"],
                    "properties_id": row.node_label
                }
                for row in tqdm(dfjkgen_nodes.itertuples(), total=len(dfjkgen_nodes), desc="Building Term objects for node labels")
            ]
        )

        # Build Terms for the node's synonyms.
        # Split on pipe delimiter.
        dfjkgen_nodes['node_synonyms'] = (dfjkgen_nodes['node_synonyms'].fillna('').str.split('|'))

        # Explode on synonyms list.
        df_exploded = (
            dfjkgen_nodes[['node_id', 'node_synonyms']]
            .explode('node_synonyms')
            .rename(columns={'node_synonyms': 'node_synonym'})
            .reset_index(drop=True)
        )
        listret.extend(
            [
                {
                    "labels": ["Term"],
                    "properties_id": row.node_synonym
                }
                for row in
                tqdm(df_exploded.itertuples(), total=len(df_exploded), desc="Building Term objects for node synonyms")
            ]
        )

        return listret

    def _get_node_base_cols(self)-> set:
        """
        Identifies base node property columns for JKGEN nodes.

        The nodes file can have a variable number of columns after the
        node_dbxrefs column.
        These columns correspond to node properties.
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
                     }

    def _build_new_coderels(self, dfjkgen_nodes: pd.DataFrame)-> list[dict]:
        """
        Builds a list of coderel objects for the JKG JSON rels array from a JKGEN node.
        :param dfjkgen_nodes: DataFrame of JKG JSON nodes data

        :return: list of coderel objects (dicts) for:
             - the node
             - the node's synonyms
        """

        list_new_coderels = []

        # Explode on linked cuis.
        df_exploded = (
            dfjkgen_nodes
            .explode('cuis')
            .rename(columns={'cuis': 'cui'})
            .reset_index(drop=True)
        )

        """
            Add any values from optional columns with a "properties_" prefix to indicate that they will
            be in the eventual nested properties object.
            e.g., if the nodes file has a column "X", the properties 
            dict will contain a key "X".

        """

        # Compute the set of optional columns once (outside the loop for efficiency)
        excluded_cols = self._get_node_base_cols()
        optional_cols = [col for col in df_exploded.columns if col not in excluded_cols]

        # Build the coderels that correspond to the PT term type.
        # Use the packing operator to add the custom properties.
        list_new_coderels.extend(
            [
                {
                    "labels": ["CODE"],
                    "start_id": row.cui,
                    "end_id": row.node_label,
                    "properties_sab": self.sab,
                    "properties_def": row.node_definition,
                    "properties_codeid": row.node_id,
                    "properties_tty": "PT",
                    # ** unpacks a dict built per-row from the optional columns
                    **{f"properties_{col}": getattr(row, col) for col in optional_cols}
                }
                for row in tqdm(df_exploded.itertuples(),
                                total=len(df_exploded),
                                desc="Building Coderel objects for nodes")

            ]
        )

        print(list_new_coderels)
        exit(1)

        # coderel for node label
        #start_id = cui
        #end_id = row['node_label']
        #properties_sab = self.sab.upper()
        #properties_def = row['node_definition']
        #properties_codeid = row['node_id']
        #properties_tty = 'PT'

        """
        Add any values from optional columns with a "properties_" prefix to indicate that they will
        be in the eventual nested properties object.
        e.g., if the nodes file has a column "X", the properties 
        dict will contain a key "X".
        
        """

        #optional_properties =  [
            #{f"properties_{col}": row[col]}
            #for col, row in dfjkgen_nodes.iterrows() if col not in self._get_custom_node_property_cols()
        #]
        # Build using the packing operator for optional properties.
        #new_coderel_pref = {
            #"labels": ["CODE"],
            #"start_id": start_id,
            #"end_id": end_id,
            #"properties_sab": properties_sab,
            #"properties_def": properties_def,
            #"properties_codeid": properties_codeid,
            #"properties_tty": properties_tty,
            #**optional_properties
        #}
        #list_new_coderels.append(new_coderel_pref)

        # Add coderels for new node's synonyms.
        # Synonyms will not have definitions or node properties.
        #synonyms = row['node_synonyms'].split('|') if pd.notna(row['node_synonyms']) else []

        #for synonym in synonyms:
            #end_id = synonym
            #properties_tty = 'SY'
            # Do not copy the definition to synonym coderels
            #properties_def = ""
            #new_coderel_syn = {
                #"labels": ["CODE"],
                #"start_id": start_id,
                #"end_id": end_id,
                #"properties_sab": properties_sab,
                #"properties_def": properties_def,
                #"properties_codeid": properties_codeid,
                #"properties_tty": properties_tty,
            #}
            #list_new_coderels.append(new_coderel_syn)

        #return list_new_coderels

    def _parse_cui_list(self, val):
        """
        Parse a cui value.
        :param val: represenation of a CUI that may be:
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
            raise TypeError("unknown type for new CUI")

    def _get_cuis_for_nodes(self, df_nodes: pd.DataFrame) -> pd.Series:
        """

        Implements the UBKG-JKG equivalence class algorithm.

        Identifies the CUIs for the concepts to which to link
        JKGEN nodes, based on a ranked evaluation of the cross-references
        (dbxrefs) for each JKGEN node.

        :param df_nodes: a DataFrame of JKGEN node information.

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
        4. the existing CUI for the code
        5. a CUI minted from the code
        """

        # This is a block operation with no hooks for tqdm,
        # so start a spinner.
        utimer = UbkgTimer(display_msg="Getting CUIs for nodes")

        """
        1. Do the following:
           a. Fill missing dbxrefs.
           b. Split the dbxrefs string on the pipe delimiter.
           c. Explode to one row per dbxref.
        
        In other words, transform the DataFrame rows  
        
            from:
            node_id node_dbxrefs
            node1   SAB1:Code1|SAB2:Code2
            
            to:
            node_id node_dbxrefs
            node1   [SAB1:Code1, SAB2:Code2]
        
            and then to:
            node_id node_dbxrefs
            node1   SAB1:Code1
            node1   SAB2:Code2
        """

        df_nodes = df_nodes.copy()

        # 1.a, 1.b
        df_nodes['node_dbxrefs'] = (df_nodes['node_dbxrefs'].fillna('').str.split('|'))
        # 1.c
        df_exploded = df_nodes[['node_id', 'node_dbxrefs']].explode('node_dbxrefs').reset_index(drop=True)

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
        if self.jkg_json_coderels.empty:
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
            df_other = (df_exploded.merge(self.jkg_json_coderels,
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


            """
            Obtain any CUIs already linked to the node_id.
            """
            df_node_cui = (df_exploded.merge(self.jkg_json_coderels,
                                          how='left',
                                          left_on='node_id',
                                          right_on='properties_codeid')
                        .rename(columns={'node_label_x': 'node_label'}))
            node_cui_map = (
                df_node_cui.groupby('node_id')['start_id']
                .apply(lambda x: x.dropna().unique().tolist())
                .to_dict()
            )

            """
            Identify CUIs for the node_id. Select the first CUI from lists 
            in order of:
            1. direct UMLS CUIs
            2. other UMLS CUIs
            3. other non-UMLS CUIs
            4. any CUIs for the node_id in coderels.
            If no CUI identified, mint a new CUI.
                
            """

        utimer.stop()

        return df_nodes['node_id'].map(
        lambda node_id: self._get_all_cuis_from_maps(
            node_id=node_id,
            direct_umls_map=direct_umls_map,
            other_umls_map=other_umls_map,
            other_non_umls_map=other_non_umls_map,
            node_cui_map=node_cui_map
        )
    )

    def _get_all_cuis_from_maps(self, node_id: str,
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

        # Look for existing CUI assignment.
        if node_cuis:
            all_cuis = all_cuis + node_cuis

        # Default: mint a new CUI for the node.
        if not (direct_umls_cuis
                or other_umls_cuis
                or other_non_umls_cuis
                or node_cuis):
            all_cuis =  [self._mint_new_cui(node_id)]

        # Get unique list of CUIs in original order, by rank.
        return list(dict.fromkeys(all_cuis))

    def _get_single_cui_from_maps(self, node_id, direct_umls_map:dict, other_umls_map:dict, other_non_umls_map:dict) -> str:

        """
        Selects a single dbxref CUI for a node, using map objects built in the
        _get_cuis_for_nodes function. Replicates the older "preferred code"
        UBKG selection logic, which will not be used in UBKG-JKG.

        :param node_id: code for a node
        :param direct_umls_map: map of nodes to direct UMLS CUIs
        :param other_umls_map: map of nodes to other UMLS CUIs
        :param other_non_umls_map: map of nodes to other non-UMLS CUIs

        Each map has keys that are codes from the JKGEN node file, with
        values that are lists of CUIs--e.g.,
        # {'UBERON:0001794':[UMLS:c1512783]...}

        :return: a CUI string
        """

        # Check the maps of code to CUIs.
        direct_umls = direct_umls_map.get(node_id, [])
        other_umls = other_umls_map.get(node_id, [])
        other_non_umls = other_non_umls_map.get(node_id, [])

        """
        Implements a waterfall/priority fallback:
        direct UMLS  →  transitive UMLS code  → transitive non-UMLS code  →  mint a brand-new CUI
        """

        if direct_umls:
            return direct_umls[0].upper()
        elif other_umls:
            return other_umls[0].upper()
        elif other_non_umls:
            return other_non_umls[0].upper()
        else:
            return self._mint_new_cui(node_id)

    def _build_jkgjson_for_jkgen_nodes(self):
        """
        Builds lists of nodes and rels objects to add to the JKG JSON
        related to the JKGEN nodes file.

        """

        # Nodes from the JGKGEN node file
        dfjkgen_nodes = self.jkgen.nodes.fillna('')

        # Get the source node for the SAB.
        self.new_jkg_json_node_sources = [self._build_sab_source_node()]

        """
        BUILD FOR NEW NODES FROM THE NODES FILE.
        For each node in the node file, 
        1. Identify the CUIs to link to the node's code.
           This involves an application of the equivalence class algorithm.
        2. For each new concept, add to the JKG JSON's nodes array a 
           Concept object with id = the new CUI and 
           pref_term = the node's label.
        3. Add to the JKG JSON's nodes array the following Term objects:
           a. a Term object with id = node_label
           b. for each synonym, a Term object with id = value from node_synonyms
        4. Add to the JKG JSON's rels array coderels (CODE relationships)
           that links the node's node_id with the node's CUIs and 
           tty = PT
        5. For each synonym of the node, add coderels that link
           the node's node_id with the node's CUIs and tty = SY.

        """

        self.ulog.print_and_logger_info('Building JKG JSON arrays for nodes in JGKEN nodes file.')

        # Apply the equivalence class algorithm to
        # identify CUIs to which to assign new nodes.
        dfjkgen_nodes['cuis'] = self._get_cuis_for_nodes(df_nodes=dfjkgen_nodes)
        cuifile = os.path.join(self.sab_jkg_dir,'node_cuis.csv')
        dfjkgen_nodes.to_csv(cuifile, index=False)

        # Initialize lists of objects by type:
        # Term nodes
        list_new_node_terms = []
        # coderel rels
        list_new_coderels = []

        # Build concept nodes for new concepts linked to nodes.
        self.new_jkg_json_node_concepts=self._build_new_concept_nodes(dfjkgen_nodes=dfjkgen_nodes)

        # Build term label nodes for the new nodes and their synonyms.
        self.new_jkg_json_node_terms =self._build_new_term_nodes(dfjkgen_nodes=dfjkgen_nodes)

        # Build coderels for the nodes and their synonyms.
        self.new_jkg_json_coderels = self._build_new_coderels(dfjkgen_nodes=dfjkgen_nodes)
        exit(1)
        # TODO: replace cuis in rels for nodes for which cuis were updated.


    def _build_jkgjson_for_jkgen_edges(self):
        """
        Translates the edges in a JKGEN edge file to a list of
        new concept-concept relationships for the
        rels array of the JKG JSON.

        """

        self.ulog.print_and_logger_info('Building JKG JSON arrays for edges in JKGEN edge file.')

        """
        Identify the subject node CUI.
        1. Merge JKGEN subject node codes against JKGJSON coderel codes
           to obtain existing CUIs for JKGEN subject nodes.
        2. Merge against new node coderels to obtain new CUIs for JKGEN subject
           nodes.
        """

        # Convert list of new coderels to a DataFrame to take
        # advantage of Pandas DataFrame merging.
        dfnewcoderels = pd.DataFrame(self.new_jkg_json_coderels)

        # The merges rename start_id to indicate that it is a CUI.

        # 1
        dfsubject = self.jkgen.edges.copy()
        if not self.jkg_json_coderels.empty:
            dfsubject = (dfsubject.merge(self.jkg_json_coderels,
                                    how='left',
                                    left_on='subject',
                                    right_on='properties_codeid')
                     .rename(columns = {'node_label_x': 'node_label',
                                        'start_id': 'original_cui',}))
        else:
            dfsubject['original_cui'] = pd.NA  # ensure column exists for fillna below

        # 2
        dfsubject = (dfsubject.merge(dfnewcoderels,
                                    how='left',
                                    left_on='subject',
                                    right_on='properties_codeid')
                     .rename(columns={'node_label_x': 'node_label',
                                      'start_id': 'new_cui'}))
        dfsubject = dfsubject.drop_duplicates(subset=['subject','predicate','object'])

        dfsubject['cui'] = dfsubject['original_cui'].fillna(dfsubject['new_cui'])

        """
        Identify the object node CUI.
        1. Merge JKGEN object node codes against JKGJSON coderel codes
           to obtain existing CUIs for JKGEN object nodes.
        2. Merge against new node coderels to obtain new CUIs for JKGEN object
           nodes.
        """
        # 1
        dfobject = self.jkgen.edges.copy()
        if not self.jkg_json_coderels.empty:
            dfobject = (dfobject.merge(self.jkg_json_coderels,
                                     how='left',
                                     left_on='object',
                                     right_on='properties_codeid')
                     .rename(columns={'node_label_x': 'node_label',
                                      'start_id': 'original_cui',}))
        else:
            dfobject['original_cui'] = pd.NA  # ensure column exists for fillna below

        # 2
        dfobject = (dfobject.merge(dfnewcoderels,
                                     how='left',
                                     left_on='object',
                                     right_on='properties_codeid')
                     .rename(columns={'node_label_x': 'node_label',
                                      'start_id': 'new_cui'}))
        dfobject = dfobject.drop_duplicates(subset=['subject','predicate','object'])
        dfobject['cui'] = dfobject['original_cui'].fillna(dfobject['new_cui'])

        # Build the rel objects.
        df_edges = self.jkgen.edges.copy()
        # Map the subject and object nodes to their respective CUIs.
        df_edges['subject_cui'] = dfsubject['cui'].values
        df_edges['object_cui'] = dfobject['cui'].values

        """
        CUSTOM EDGE PROPERTIES
        The edge file has a variable number of columns that
        represent edge properties. Include these as flattened
        members of a "properties" object. 
        
        For each optional column, there will be a corresponding
        "properties_" field. This field will eventually be 
        "unflattened and added to a properties dict in an node object.
        
        """

        # Exclude columns that either will be translated or that
        # are artifacts of analysis.
        base_cols = {'subject', 'predicate', 'object', 'subject_cui', 'object_cui'}

        extra_cols = [c for c in df_edges.columns if c not in base_cols]

        # Vectorized build, using packing operator.
        # Note that the key for node objects is "label", not "labels".
        self.new_jkg_json_rels = df_edges.apply(lambda row: {
            "label": row['predicate'],
            "start_id": row['subject_cui'],
            "end_id": row['object_cui'],
            "properties_sab": self.sab,
            **{f"properties_{c}": row[c] for c in extra_cols},
            "properties_id": f'{self.sab}:{row['predicate']}'
        }, axis=1).tolist()

    def _unflatten_objects(self, list_flat_objects: list) -> list:
        """
        Converts a list of flattened JKG JSON objects into a
        "unflattened", or nested, JKG JSON array.

        :param list_flat_objects: list of flattened JKG JSON objects
        :return: JKG JSON array

        """

        list_unflat_objects = []
        for flat_object in list_flat_objects:

            # Move all key/value pairs for which the key starts with "properties_",
            # "start_", or "end_to nested dicts.
            properties = {k.removeprefix('properties_'): v
                      for k, v in flat_object.items() if k.startswith('properties_')}
            start = {k.removeprefix('start_'): v
                          for k, v in flat_object.items() if k.startswith('start_')}
            end = {k.removeprefix('end_'): v
                     for k, v in flat_object.items() if k.startswith('end_')}
            unflat_object = {k: v for k, v in flat_object.items()
                             if not k.startswith(('properties_', 'start_', 'end_'))}
            # Add nested dicts.
            if properties != {}:
                unflat_object['properties'] = properties
            if start != {}:
                unflat_object['start'] = {"properties": start}
            if end != {}:
                unflat_object['end'] = {"properties": end}


            list_unflat_objects.append(unflat_object)

        return list_unflat_objects

    def _build_new_jkgjson(self):

        """
        Adds nodes, coderels, and rels lists to the existing JKG JSON.

        """
        self.ulog.print_and_logger_info('Building new JKG JSON file.')

        # BUILD rels ARRAY
        self.ulog.print_and_logger_info("Building rels array.")

        # Convert the DataFrame of flattened original JKG JSON rels into a list of dicts.
        list_flat_jkg_json_rels = self.jkgjson.rels.to_dict(orient='records')
        if len(list_flat_jkg_json_rels) == 0:
            list_flat_jkg_json_rels = []

        # Combine the list of flattened original JKG JSON rels with the lists of
        # flattened new JGKEN rels and coderels.
        list_flat_rels = list_flat_jkg_json_rels + self.new_jkg_json_rels + self.new_jkg_json_coderels

        # "Unflatten" the elements of the combined flattend list--i.e.,
        # convert flattened objects to nested, or "unflattened" dicts.
        utimer = UbkgTimer(display_msg="Unflattening rels array.")
        list_nested_jkg_json_rels = self._unflatten_objects(list_flat_objects=list_flat_rels)

        utimer.stop()

        """
        BUILD nodes ARRAY
        
        The new sources node for the SAB is already unflattened.
        It is necessary to insert the new sources node between
        the old sources node and the remaining nodes.
        """

        utimer = UbkgTimer(display_msg="Building nodes array.")
        # Convert the DataFrame of flattened original JKG JSON source nodes into a list of dicts.
        list_flat_jkg_json_sources = self.jkg_json_sources.to_dict(orient='records')
        if len(list_flat_jkg_json_sources) == 0:
            list_flat_jkg_json_sources = []

        # Convert the DataFrame of flattened original JKG JSON "not source" nodes into a list of dicts.
        list_flat_jkg_json_not_sources = self.jkg_json_not_sources.to_dict(orient='records')
        if len(list_flat_jkg_json_not_sources) == 0:
            list_flat_jkg_json_not_sources = []

        # Unflatten the flattened list of original JKG JSON source nodes.
        list_nested_jkg_json_source_nodes = self._unflatten_objects(list_flat_objects=list_flat_jkg_json_sources)
        # Unflatten the flattened list of original JKG JSON nodes other than source.
        list_nested_jkg_json_not_source_nodes = self._unflatten_objects(list_flat_objects=list_flat_jkg_json_not_sources)

        # Unflatten the flattened lists of new JKGGEN concept and term nodes.
        list_nested_new_jkg_json_concept_nodes = self._unflatten_objects(list_flat_objects=self.new_jkg_json_node_concepts)
        list_nested_new_jkg_json_term_nodes = self._unflatten_objects(list_flat_objects=self.new_jkg_json_node_terms)

        utimer.stop()

        """
        Combine all unflattened nodes in order:
        1. Original JKG JSON source nodes
        2. New JKG JSON source node
        3. Original JKG JSON non-source nodes:
           a. Node_label
           b. Rel_label
           c. Concept
           d. Term
        4. New JKG JSON Concept nodes
        5. New JKG JSON Term nodes
        """

        list_nested_jkg_json_nodes = (list_nested_jkg_json_source_nodes +
                                      self.new_jkg_json_node_sources +
                                      list_nested_jkg_json_not_source_nodes +
                                      list_nested_new_jkg_json_concept_nodes +
                                      list_nested_new_jkg_json_term_nodes)

        # WRITE nodes AND rels ARRAYS TO A NEW JKG JSON.

        outpath = os.path.join(self.jkgjson_dir, 'new_jkg.json')

        # Use a JsonWriter object to build the new version of JKGJSON.
        jw = JsonWriter(outpath=outpath)
        # Start the JSON.
        jw.start_json()

        # Write the nodes array.
        jw.start_list(keyname='nodes')
        jw.write_list(list_name='nodes', list_content=list_nested_jkg_json_nodes)
        jw.end_list()

        # Add delimiters.
        jw.write_comma()
        jw.write_line_feed()

        # Write the rels array.
        jw.start_list(keyname='rels')
        jw.write_list(list_name='rels', list_content=list_nested_jkg_json_rels)
        jw.end_list()

        # End the JSON.
        jw.end_json()


