"""
jkg_import.py
Sabjkgimport class that adds new nodes and rels objects to an existing
JKG JSON
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
        self.repo_root = repo_root
        # UBKG code and relationship standardizer object
        self.ustand = ubkgStandardizer(ulog=ulog, repo_root=repo_root)
        # UBKG-JKG sources manager
        self.usource = ubkgSources(ulog=ulog, cfg=cfg, repo_root=repo_root)

        # Load the nodes and rels arrays from the JKG JSON.
        self.jkgjson = Jkgjson(log=ulog, cfg=cfg)

        # Get "coderel" rel objects from the flattened rels array of the JKG JSON.
        if self.jkgjson.rels.empty:
            self.jkg_json_coderels = pd.DataFrame()
        else:
            self.jkg_json_coderels = self.jkgjson.rels[self.jkgjson.rels['label'] == 'CODE']

        """
        Get source node objects from the flattened nodes array of the JKG JSON.
        This requires some Pandas trickery.
        Source nodes have a 'labels' column that is a string list--e.g.,
        "labels: ["Source"]
        So:
        1. Cast as string (required for some mystery reason)
        2. Apply ast's literal_eval to convert to a list of strings
        3. Check the first value. 
        """
        if self.jkgjson.nodes.empty:
            self.jkg_json_sources = pd.DataFrame()
            self.jkg_json_not_sources = pd.DataFrame()
        else:
            utimer = UbkgTimer(display_msg="Separating Source node objects")
            self.jkg_json_sources = self.jkgjson.nodes[self.jkgjson.nodes['labels']
                                                       .astype(str)
                                                       .apply(ast.literal_eval)
                                                       .str[0] == "Source"]
            self.jkg_json_not_sources = self.jkgjson.nodes[self.jkgjson.nodes['labels']
                                                       .astype(str)
                                                       .apply(ast.literal_eval)
                                                       .str[0] != "Source"]
            utimer.stop()

        # Load the JKGEN edge and node files.
        self.jkgen_dir = os.path.join(repo_root, cfg.get_value(section="directories", key="sab_jkg_dir"), sab)
        self.jkgjson_dir = os.path.join(repo_root, cfg.get_value(section="jkg_json", key="jkg_json_dir"))
        self.jkgen = Jkgedgenode(log=ulog, cfg=cfg, sab=sab, filedir=self.jkgen_dir)

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
        self.new_jkg_json_node_sources = []
        self.new_jkg_json_node_concepts = []
        self.new_jkg_json_node_terms = []
        self.new_jkg_json_coderels = []
        self.new_jkg_json_rels = []


        # Build lists of components for the new JKG JSON nodes array,
        # separated by type.
        self._build_jkgjson_for_jkgen_nodes()

        # Build a list of components for the new JKG JSON rels array.
        self._build_jkgjson_for_jkgen_edges()

        # Add new nodes and rels objects to the JKG JSON and write to output.
        self._build_new_jkgjson()

        #self._write_new_jkgjson_arrays()

    def _build_sab_source_node(self) -> dict:
        """
        Builds a JKG JSON Source node for a non-UMLS SAB.
        """
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

    def build_new_concept_node(self, cui:str, row:pd.Series) -> dict:
        """
        Builds an object for the JKG JSON nodes array from a JKGEN node.
        :param cui: CUI for the new JKGEN node's concept
        :param row: row of node information
        :return: dict for the Concept label
        """

        # The only label for a new Concept node from a JKGEN node
        # is "Concept".
        return {
            "labels": ["Concept"],
            "properties_id": cui,
            "properties_pref_term": row['node_label'],
            "properties_sab": self.sab.upper()
        }

    def build_new_term_nodes(self, row:pd.Series) -> list[dict]:
        """
        Builds Term objects for the JKG JSON nodes array from a JKGEN node.
        :param row: row of node information
        :return: list of Term objects (dicts) for:
                 - the node
                 - the node's synonyms
        """
        listret = []

        # Keys with the "properties_" index will eventually
        # be "unflattened" and moved to a nested dict.
        # Term label node for new node
        new_node_term_pref = {
            "labels": ["Term"],
            "properties_id": row["node_label"]
        }
        listret.append(new_node_term_pref)

        # Term labels for node's synonyms
        synonyms = row['node_synonyms'].split('|') if pd.notna(row['node_synonyms']) else []
        for synonym in synonyms:
            new_node_term_syn = {
                "labels": ["Term"],
                "properties_id": synonym
            }
            listret.append(new_node_term_syn)

        return listret

    def build_new_coderels(self, cui:str, row:pd.Series)-> list[dict]:
        """
            Builds a list of objects for the JKG JSON rels array from a JKGEN node.

            :param cui: CUI for the new JKGEN node's concept
            :param row: row of node information
            :return: list of rel objects (dicts) for:
                 - the node
                 - the node's synonyms

            Development note:
            This is called in an external routine that loops over the
            rows in a DataFrame. This routine cannot be vectorized because
            the external routine uses conditional logic (the "business
            rule") to identify the value of the cui parameter.
        """

        # Keys with prefixes "properties_", "start_", and "end_" will
        # eventually be "unflattened" into nested dicts.

        list_new_coderels = []

        # coderel for node label
        start_id = cui
        end_id = row['node_label']
        properties_sab = self.sab.upper()
        properties_def = row['node_definition']
        properties_codeid = row['node_id']
        properties_tty = 'PT'

        # The nodes file can have a variable number of columns after the
        # node_dbxrefs column whose values correspond to node properties.
        # Add these with a "properties_" prefix to indicate that they will
        # be in the eventual nested properties object.
        base_cols = {'node_id',
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
        optional_properties = {f"properties_{col}": row[col]
                               for col in row.index if col not in base_cols}

        # Build using the packing operator for optional properties.
        new_coderel_pref = {
            "labels": ["CODE"],
            "start_id": start_id,
            "end_id": end_id,
            "properties_sab": properties_sab,
            "properties_def": properties_def,
            "properties_codeid": properties_codeid,
            "properties_tty": properties_tty,
            **optional_properties
        }
        list_new_coderels.append(new_coderel_pref)

        # Add coderels for new node's synonyms.
        # Synonyms do not have definitions or node properties.
        synonyms = row['node_synonyms'].split('|') if pd.notna(row['node_synonyms']) else []
        for synonym in synonyms:
            properties_tty = 'SY'
            # Do not copy the definition to synonym coderels
            properties_def = ""
            new_coderel_syn = {
                "labels": ["CODE"],
                "start_id": start_id,
                "end_id": end_id,
                "properties_sab": properties_sab,
                "properties_def": properties_def,
                "properties_codeid": properties_codeid,
                "properties_tty": properties_tty,
            }
            list_new_coderels.append(new_coderel_syn)

        return list_new_coderels

    def _parse_cui_list(self, val):
        """Parse a cui value that may be a list string, empty list string, or NaN."""
        if pd.isna(val) if not isinstance(val, list) else False:
            return []
        if isinstance(val, list):
            return val
        try:
            parsed = ast.literal_eval(val)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, SyntaxError):
            return []

    def _get_cui_for_node(self, row: pd.Series)-> str:

        """
        Identifies the CUI for the concept with which to associate
        a JKGEN node, based on a ranked evaluation of cross-references
        (dbxrefs) for the JKGEN node.

        :param row: row of node information

        """

        """
        The dbxref column of the row is a pipe-delimited list of cross-references.
        
        Example: 
        node_id           dbxrefs
        UBERON:0001748    emapa:35663|fma:55566|umls:c0927176|ma:0002676|ncit:c33265
        UBERON:0005030    fma:59772
        MP:0011739        cl:0002084
        
        Cross-references can be of the following types:
        1. A UMLS CUI (e.g., umls:c0927176). The dbxref is a "direct equivalence".
        2. Codes linked to CUIs that are in the JKG JSON. The dbxref is a
           "transitive equivalence".
           Transitive codes are of two types:
           a. A code from a UMLS vocabulary that has a dbxref to a UMLS CUI.
              In the case of UBERON:0005030, FMA is a UMLS vocabulary, 
              and fma:55566 has a UMLS CUI. 
           b. A code from a non-UMLS vocabulary that was ingested prior to the
              current ingestion, and so has a non-UMLS CUI. 
              This is known as an "other CUI". MP:0011739 is an example: its
              dbxref is to CL:0002084. When CL is ingested prior to MP,
              it has a CUI ("CL:0002084 CUI") in JKGJSON.

        The available CUIs for a code from a vocabulary (SAB)
        are ranked in order of preference:
        1. the first direct UMLS CUI 
        2. the first UMLS CUI for a transitive code 
        3. the first other CUI for a transitive code
        4. a CUI minted from the code
        
        Assigning a CUI to a code based on this order has the intent of
        linking a code to the CUI that has the highest number
        of codes linked to it.
        
        The current business rule for dbxrefs is that only one code from a 
        vocabulary (SAB) should have a dbxref to a particular CUI. 
        If more than one code from a SAB has a dbxref to the same 
        CUI (i.e., the SAB has higher code resolution than the other
        vocabulary), then codes are assigned to CUIS based on their order in the 
        ingestion.
        
        For example, in the MP JKGEN node file, MP:0010169, MP:0008397, and MP:0010168 have dbxref of 
        cl:0000792. 
        1. MP:0010169 is assigned to the CUI for CL:0000792.
        2. MP:0008397 and MP:0010168 are assigned to their own CUIs.
        
        This business rule currently breaks concept-code synonymy for all but the first 
        code in a set of codes in a SAB that map to the same CUI.
        
        """

        """
        Convert the dbxref strings to a list, splitting on the pipe delimiter.
        e.g., node1 / SAB1:Code1|SAB2:Code2 => node1/ [SAB1:Code1, SAB2:Code2]
        """

        row['node_dbxrefs'] = row['node_dbxrefs'].split('|') if pd.notna(row['node_dbxrefs']) else []

        # If there are no dbxrefs, mint a CUI.
        if not row['node_dbxrefs']:
            return self._mint_new_cui(code=row['node_id'])

        """
        Explode the row to a DataFrame in which each row maps the code to a single cross-reference
        from the list.
        e.g.,
        from
            nodeID / node_dbxrefs
            node1/ [SAB1:Code1, SAB2:Code2]
        to
            nodeID / node_dbxrefs
            node1 / SAB1:Code1
            node1 / SAB2:Code2
        """
        df_by_dbxref = pd.Series(row).to_frame().T.explode('node_dbxrefs').reset_index(drop=True)


        # Drop any empty strings from the split (e.g. trailing pipes)
        df_by_dbxref = df_by_dbxref[df_by_dbxref['node_dbxrefs'].str.strip() != '']

        # Standardize the codes for the dbxrefs, using the SAB for the dbxref.
        # The standardize_code function expects a Pandas series.
        df_by_dbxref['sab'] = df_by_dbxref['node_dbxrefs'].str.split(':').str[0]
        df_by_dbxref['node_dbxrefs'] = df_by_dbxref.apply(
            lambda r: self.ustand.standardize_code(x=pd.Series([r['node_dbxrefs']]), sab=r['sab'])[0],
            axis=1
        )

        # Pivot column: identify direct UMLS CUI dbxrefs.
        # Do not sort by dbxref.
        df_direct_umls = (
            df_by_dbxref[df_by_dbxref['node_dbxrefs'].str.lower().str.startswith('umls:')]
            .groupby('node_id', sort=False)['node_dbxrefs']
            .apply(list)
            .reset_index(name='direct_cuis')
        )
        df_by_dbxref = df_by_dbxref.merge(df_direct_umls,
                                            how='left',
                                            on='node_id')

        # Identify CUIs from other vocabularies.
        # These are from coderels objects from the JKG JSON.
        # Merge against the codeid property of the coderel.

        df_transitive_umls = (df_by_dbxref.merge(self.jkg_json_coderels,
                                                how='left',
                                                left_on='node_dbxrefs',
                                                right_on='properties_codeid')
                              .rename(columns={'node_label_x': 'node_label'}))
        # Build a list of CUIs, grouped by the dbxref code.
        transitive_map = (
            df_transitive_umls.groupby('node_dbxrefs')['start_id']
            .apply(lambda x: x.dropna().unique().tolist())
        )
        # Pivot column: list of transitive CUIs.
        df_by_dbxref['transitive_cuis'] = df_by_dbxref['node_dbxrefs'].map(transitive_map)

        # Pivot column: identify a CUI for the code.
        df_by_dbxref['self_cui'] = self._mint_new_cui(code=df_by_dbxref['node_id'])

        # Get CUI lists by type.
        direct_cui = self._parse_cui_list(df_by_dbxref['direct_cuis'].iloc[0])
        transitive_cui = self._parse_cui_list(df_by_dbxref['transitive_cuis'].iloc[0])
        self_cui = df_by_dbxref['self_cui'].iloc[0]

        if direct_cui is not None and len(direct_cui) > 0:
            cui = direct_cui[0].upper()
        elif transitive_cui is not None and len(transitive_cui) > 0:
            cui = transitive_cui[0].upper()
        else:
            cui = self_cui

        return cui

    def _mint_new_cui(self, code:Any) -> str:
        """
        Mints a new CUI based on a code.
        :param code:
        """
        if type(code) == str:
            return code + ' CUI'
        elif type(code) == pd.Series:
            return code.iloc[0] + ' CUI'
        else:
            raise TypeError("unknown type for new CUI")

    def _get_cuis_for_node(self, df_nodes: pd.DataFrame) -> pd.Series:
        """
        Vectorized version of _get_cui_for_node.
        Processes all nodes at once instead of row by row.

        Identifies the CUIs for the concepts to which to link
        JKGEN nodes, based on a ranked evaluation of the cross-references
        (dbxrefs) for each JKGEN node.

        ---
        The dbxref column of the row is a pipe-delimited list of cross-references.

        Example:
        node_id           node_dbxrefs
        UBERON:0001748    emapa:35663|fma:55566|umls:c0927176|ma:0002676|ncit:c33265
        UBERON:0005030    fma:59772
        MP:0011739        cl:0002084

        Cross-references can be of the following types:
        1. A UMLS CUI (e.g., umls:c0927176). The dbxref is a "direct equivalence".
        2. Codes linked to CUIs that are in the JKG JSON. The dbxref is a
           "transitive equivalence".
           Transitive codes are of two types:
           a. A code from a UMLS vocabulary that has a dbxref to a UMLS CUI.
              In the case of UBERON:0005030, FMA is a UMLS vocabulary,
              and fma:55566 has a UMLS CUI.
           b. A code from a non-UMLS vocabulary that was ingested prior to the
              current ingestion, and so has a non-UMLS CUI.
              This is known as an "other CUI". MP:0011739 is an example: its
              dbxref is to CL:0002084. When CL is ingested prior to MP,
              it has a CUI ("CL:0002084 CUI") in JKGJSON.

        The available CUIs for a code from a vocabulary (SAB)
        are ranked in order of preference:
        1. the first direct UMLS CUI
        2. the first UMLS CUI for a transitive code
        3. the first other CUI for a transitive code
        4. a CUI minted from the code

        Assigning a CUI to a code based on this order has the intent of
        linking a code to the CUI that has the highest number
        of codes linked to it.

        The current business rule for dbxrefs is that only one code from a
        vocabulary (SAB) should have a dbxref to a particular CUI.
        If more than one code from a SAB has a dbxref to the same
        CUI (i.e., the SAB has higher code resolution than the other
        vocabulary), then codes are assigned to CUIS based on their order in the
        ingestion.

        For example, in the MP JKGEN node file, MP:0010169, MP:0008397, and MP:0010168 have dbxref of
        cl:0000792.
        1. MP:0010169 is assigned to the CUI for CL:0000792.
        2. MP:0008397 and MP:0010168 are assigned to their own CUIs.

        This business rule currently breaks concept-code synonymy for all but the first
        code in a set of codes in a SAB that map to the same CUI.
        """

        # Block operation, so start a spinner.
        utimer = UbkgTimer(display_msg="Getting CUIs for nodes")

        """
            1. Do the following:
               a. Fill missing dbxrefs.
               b. Split the dbxrefs string on the pipe delimiter.
               c. Explode to one row per dbxref.
               d. Remove trailing pipe delimiter.
        
                from:
                
                node_id node_dbxrefs
                node1   SAB1:Code1|SAB2:Code2
                
                to:
                node_id node_dbxrefs
                node1   [SAB1:Code1, SAB2:Code2]
                
                to:
                node_id node_dbxrefs
                node1   SAB1:Code1
                node1   SAB2:Code2
        """

        df_nodes = df_nodes.copy()
        # 1.a, 1.b
        df_nodes['node_dbxrefs'] = (df_nodes['node_dbxrefs'].fillna('').str.split('|'))
        # 1.c
        df_exploded = df_nodes[['node_id', 'node_dbxrefs']].explode('node_dbxrefs').reset_index(drop=True)
        # 1.d
        df_exploded = df_exploded[df_exploded['node_dbxrefs'].str.strip() != '']

        """
        2. Standardize dbxref codes, grouping by SAB. 
           (The standardization of a code depends on the code SAB.)
           a. Extract SABs from dbxref codes.
           b. Convert each dbxref into a series to standardize the code.
           c. Group standardized codes by SAB.
           d. Collect standardized codes into lists.
        """
        # 2a.
        df_exploded['sab'] = df_exploded['node_dbxrefs'].str.split(':').str[0]
        # 2b-d.
        if df_exploded.empty:
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
           b. Group by node_id and collect UMLS CUIs into lists.
        """
        # 3a.
        df_direct_umls = df_exploded[df_exploded['node_dbxrefs'].str.lower().str.startswith('umls:')]
        # 3b.
        direct_umls_map = (
            df_direct_umls.groupby('node_id', sort=False)['node_dbxrefs']
            .apply(list)
        )

        """
        4. Identify "other CUIs"--dbxrefs with codes that have CUIs in coderels.
           a. Merge against coderels.
           b. Group by node_id and collect CUIs into lists.
           c. Distinguish CUIs by whether they are from the UMLS or a non-UMLS SAB.
        """
        if self.jkg_json_coderels.empty:
            df_other = df_exploded
            other_umls_map = {}
            other_non_umls_map = {}
        else:
            df_other = (df_exploded.merge(self.jkg_json_coderels,
                                          how='left',
                                          left_on='node_dbxrefs',
                                          right_on='properties_codeid')
                        .rename(columns={'node_label_x': 'node_label'}))

            # 4b. Split into UMLS and non-UMLS
            # Note the filter on dbxrefs not including UMLS
            df_other_umls = df_other[
                df_other['start_id'].str.startswith('UMLS', na=False) &
                ~df_other['node_dbxrefs'].str.upper().str.startswith('UMLS', na=False)
                ]
            df_other_non_umls = df_other[~df_other['start_id'].str.startswith('UMLS', na=False)]

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
        5. Select CUI per node_id. Select the first CUI from lists 
           in order of:
           1. direct UMLS CUIs
           2. other UMLS CUIs
           3. other non-UMLS CUIs
           If no CUI identified, mint a new CUI.
        """

        def pick_cui(node_id):
            direct_umls = direct_umls_map.get(node_id, [])
            other_umls = other_umls_map.get(node_id, [])
            other_non_umls = other_non_umls_map.get(node_id, [])
            if direct_umls:
                return direct_umls[0].upper()
            elif other_umls:
                return other_umls[0].upper()
            elif other_non_umls:
                return other_non_umls[0].upper()
            else:
                return self._mint_new_cui(node_id)

        utimer.stop()
        return df_nodes['node_id'].map(pick_cui)

    def _build_jkgjson_for_jkgen_nodes(self):
        """
        Builds lists of nodes and rels objects to add to the JKG JSON
        related to the JKGEN nodes file.

        """

        # Nodes from the JGKGEN node file
        dfjkgen_nodes = self.jkgen.nodes

        """
        Get JKGEN node_ids that are not already 
        assigned to concepts in JKG JSON via CODE relationship.
        1. Merge against the coderels frame.
        2. Restore the "label" column.
        """

        if self.jkg_json_coderels.empty:
            dfjkgen_new_nodes = dfjkgen_nodes.copy()
        else:
            dfjkgen_new_nodes = ((dfjkgen_nodes.merge(self.jkg_json_coderels,
                                                  how='left',
                                                  left_on='node_id',
                                                  right_on='properties_codeid',
                                                  indicator=True)
                              .query('_merge == "left_only"'))
                             .drop(columns=['_merge', 'properties_codeid'])
                             .rename(columns={'node_label_x': 'node_label'}))

        # Filter out nodes that only have node_id in the node file.

        dfjkgen_new_nodes = (dfjkgen_new_nodes[
            dfjkgen_new_nodes['node_label'].notna() &
            (dfjkgen_new_nodes['node_label'].str.strip() != '')
            ].drop_duplicates(subset=['node_label','node_id']))

        """
        For each new node, 
        1. Identify the CUI to link to the node's code.
        2. If the CUI is new, add to the JKG JSON's nodes array a 
           Concept object with id = the new CUI and 
           pref_term = the node's label.
        3. Add to the JKG JSON's nodes array the following Term objects:
           a. a Term object with id = node_label
           b. for each synonym, a Term object with id = value from node_synonyms
        4. Add to the JKG JSON's rels array a coderel (CODE relationship)
           that links the node's node_id with the node's CUI and 
           tty = PT
        5. For each synonym of the node, add a coderel that links
           the node's node_id with the node's CUI and tty = SY.

        """

        self.ulog.print_and_logger_info('Building JKG JSON arrays for new nodes in JGKEN nodes file.')

        # Get the source node for the SAB.
        new_node_source = self._build_sab_source_node()

        # Keep track of the concept CUI for nodes.
        # For the case of multiple nodes from a SAB
        # mapped to the same CUI, only the first node from
        # the SAB links to the CUI.
        cui = 'start'

        # Identify CUIs to which to assign new nodes.
        dfjkgen_new_nodes['cui'] = self._get_cuis_for_node(df_nodes=dfjkgen_new_nodes)


        # Initialize lists of objects by type:
        # Concept nodes
        list_new_node_concepts = []
        # Term nodes
        list_new_node_terms = []
        # coderel rels
        list_new_coderels = []

        for index, row in tqdm(dfjkgen_new_nodes.iterrows(), total=dfjkgen_new_nodes.shape[0], desc="Building node objects"):

            # Get the CUI for new node's Concept, based on dbxref.
            new_node_cui = row['cui']
            if new_node_cui == cui:
                # This is a subsequent node from the SAB that shares a CUI.
                # Mint a new CUI.
                new_node_cui = self._mint_new_cui(code=row['node_id'])
            else:
                # This is the first node from the SAB that shares a CUI.
                cui = new_node_cui

            # If the CUI is new, create a new Concept label node.
            if new_node_cui == self._mint_new_cui(code=row['node_id']):
                list_new_node_concepts.append(self.build_new_concept_node(cui=new_node_cui, row=row))

            # Term label nodes for the new node and its synonyms
            list_new_node_terms = list_new_node_terms + self.build_new_term_nodes(row=row)


            # List of coderels for the node and its synonyms.
            list_new_coderels = list_new_coderels + self.build_new_coderels(cui=new_node_cui, row=row)

        # Collect nodes and rels.
        # Convert the source node to a list for later list addition.
        self.new_jkg_json_node_sources = [new_node_source]
        self.new_jkg_json_node_concepts = list_new_node_concepts
        self.new_jkg_json_node_terms = list_new_node_terms
        self.new_jkg_json_coderels = list_new_coderels

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
        # Convert list of new coderels to a DataFrame for merging.
        dfnewcoderels = pd.DataFrame(self.new_jkg_json_coderels)

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
        # Map the subject and object CUIs.
        df_edges['subject_cui'] = dfsubject['cui'].values
        df_edges['object_cui'] = dfobject['cui'].values

        # The edge file has a variable number of columns that
        # represent edge properties. Include these as flattened
        # members of a "properties" object.

        # Exclude columns that either will be translated or that
        # are artifacts of analysis.
        base_cols = {'subject', 'predicate', 'object', 'subject_cui', 'object_cui'}

        extra_cols = [c for c in df_edges.columns if c not in base_cols]

        # Vectorized build, using packing operator.
        # Note that the key is "label", not "labels".
        self.new_jkg_json_rels = df_edges.apply(lambda row: {
            "label": row['predicate'],
            "start_id": row['subject_cui'],
            "end_id": row['object_cui'],
            "properties_sab": self.sab,
            **{f"properties_{c}": row[c] for c in extra_cols}
        }, axis=1).tolist()


    def _unflatten_objects(self, list_flat_objects: list) -> list:
        """
        Converts a list of flattened JKG JSON objects into a
        JKG JSON array.
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
        self.ulog.print_and_logger_info('Building rels array.')
        # Convert the DataFrame of flattened original JKG JSON rels into a list of dicts.
        list_flat_jkg_json_rels = self.jkgjson.rels.to_dict(orient='records')
        if len(list_flat_jkg_json_rels) == 0:
            list_flat_jkg_json_rels = []

        # Combine the list of flattened original JKG JSON rels with the lists of
        # flattened new JGKEN rels and coderels.
        list_flat_rels = list_flat_jkg_json_rels + self.new_jkg_json_rels + self.new_jkg_json_coderels

        # "Unflatten" the elements of the combined flattend list--i.e.,
        # convert flattened objects to nested, or "unflattened" dicts.
        list_nested_jkg_json_rels = self._unflatten_objects(list_flat_objects=list_flat_rels)

        # BUILD nodes ARRAY
        # The new sources node for the SAB is already nested.
        # It is necessary to insert the new sources node separately.

        self.ulog.print_and_logger_info('Building nodes array')
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

    def _write_new_jkgjson_arrays(self):

            # Write out nodes and rels arrays for the new JKGEN nodes.
            # Used for debugging.

            self.ulog.print_and_logger_info('Writing new JKG JSON nodes array')
            f = os.path.join(self.jkgen_dir, 'nodes.json')
            with open(file=f, mode='w', encoding='utf-8') as f:
                json.dump(self.new_jkg_json_nodes, f)

            self.ulog.print_and_logger_info('Writing new JKG JSON rels array')
            f = os.path.join(self.jkgen_dir, 'rels.json')
            with open(file=f, mode='w', encoding='utf-8') as f:
                json.dump(self.new_jkg_json_rels, f)

            self.ulog.print_and_logger_info('Writing new JKG JSON coderels array')
            f = os.path.join(self.jkgen_dir, 'coderels.json')
            listallrels = self.new_jkg_json_coderels
            with open(file=f, mode='w', encoding='utf-8') as f:
                json.dump(listallrels, f)

