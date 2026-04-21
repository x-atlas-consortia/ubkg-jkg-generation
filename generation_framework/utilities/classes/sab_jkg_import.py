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

from .ubkg_standardizer import ubkgStandardizer
from .ubkg_timer import UbkgTimer


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

        # Load the nodes and rels arrays from the JKG JSON.
        self.jkgjson = Jkgjson(log=ulog, cfg=cfg)

        # Load the JKGEN edge and node files.
        self.jkgen_dir = os.path.join(repo_root, cfg.get_value(section="directories", key="sab_jkg_dir"), sab)
        self.jkgen = Jkgedgenode(log=ulog, cfg=cfg, sab=sab, filedir=self.jkgen_dir)

        self.new_jkg_json_nodes = []
        self.new_jkg_json_coderels = []
        self.new_jkg_json_rels = []

        # "Coderel" objects from the flattened rels array of the JKG JSON
        self.jkg_json_coderels = self.jkgjson.jkg_rels[self.jkgjson.jkg_rels['label'] == 'CODE']

        self._build_jkgjson_for_jkgen_nodes()

        self._build_jkgjson_for_jkgen_edges()

        self._write_new_jkgjson_arrays()

    def _new_node_concept(self, cui:str, row:pd.Series) -> dict:
        """
        Builds an object for the JKG JSON nodes array from a JKGEN node.
        :param cui: CUI for the new JKGEN node's concept
        :param row: row of node information
        :return: dict for the Concept label
        """

        return {
            "labels": ["Concept"],
            "properties_id": cui,
            "properties_pref_term": row['label'],
            "properties_sab": self.sab.upper()
        }

    def _new_node_terms(self, row:pd.Series) -> list[dict]:
        """
        Builds Term objects for the JKG JSON nodes array from a JKGEN node.
        :param row: row of node information
        :return: list of Term objects (dicts) for:
                 - the node
                 - the node's synonyms
        """
        listret = []

        # Term label node for new node
        new_node_term_pref = {
            "labels": ["Term"],
            "properties_id": row["label"]
        }
        listret.append(new_node_term_pref)

        # Term labels for node's synonyms
        synonyms = row['synonyms'].split('|') if pd.notna(row['synonyms']) else []
        for synonym in synonyms:
            new_node_term_syn = {
                "labels": ["Term"],
                "properties_id": synonym
            }
            listret.append(new_node_term_syn)

        return listret

    def _new_node_coderels(self, cui:str, row:pd.Series)-> list[dict]:
        """
            Builds a list of objects for the JKG JSON rels array from a JKGEN node.

            :param cui: CUI for the new JKGEN node's concept
            :param row: row of node information
            :return: list of rel objects (dicts) for:
                 - the node
                 - the node's synonyms
        """

        list_new_node_coderels = []

        # coderel for node label
        start_id = cui
        end_id = row['label']
        properties_sab = self.sab.upper()
        properties_def = row['definitions'].split('|')[0] if pd.notna(row['definitions']) else []
        properties_codeid = row['node_id']
        properties_tty = 'PT'
        new_node_coderel_pref = {
            "labels": ["CODE"],
            "start_id": start_id,
            "end_id": end_id,
            "properties_sab": properties_sab,
            "properties_def": properties_def,
            "properties_codeid": properties_codeid,
            "properties_tty": properties_tty,
        }
        list_new_node_coderels.append(new_node_coderel_pref)

        # coderels for new node's synonyms
        synonyms = row['synonyms'].split('|') if pd.notna(row['synonyms']) else []
        for synonym in synonyms:
            properties_tty = 'SY'
            new_node_coderel_pref = {
                "labels": ["CODE"],
                "start_id": start_id,
                "end_id": end_id,
                "properties_sab": properties_sab,
                "properties_def": properties_def,
                "properties_codeid": properties_codeid,
                "properties_tty": properties_tty,
            }
            list_new_node_coderels.append(new_node_coderel_pref)

        return list_new_node_coderels

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

    def _get_CUI_for_node(self, row: pd.Series)-> str:

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
        1. the first UMLS CUI 
        2. the first other CUI
        3. a CUI minted from the code
        
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

        row['dbxrefs'] = row['dbxrefs'].split('|') if pd.notna(row['dbxrefs']) else []

        # If there are no dbxrefs, mint a CUI.
        if not row['dbxrefs']:
            return self._mint_new_CUI(code=row['node_id'])

        """
        Explode the row to a DataFrame in which each row maps the code to a single cross-reference
        from the list.
        e.g.,
        from
            nodeID / dbxrefs
            node1/ [SAB1:Code1, SAB2:Code2]
        to
            nodeID / dbxrefs
            node1 / SAB1:Code1
            node1 / SAB2:Code2
        """
        df_by_dbxref = pd.Series(row).to_frame().T.explode('dbxrefs').reset_index(drop=True)


        # Drop any empty strings from the split (e.g. trailing pipes)
        df_by_dbxref = df_by_dbxref[df_by_dbxref['dbxrefs'].str.strip() != '']

        # Standardize the codes for the dbxrefs, using the SAB for the dbxref.
        # The standardize_code function expects a Pandas series.
        df_by_dbxref['sab'] = df_by_dbxref['dbxrefs'].str.split(':').str[0]
        df_by_dbxref['dbxrefs'] = df_by_dbxref.apply(
            lambda r: self.ustand.standardize_code(x=pd.Series([r['dbxrefs']]), sab=r['sab'])[0],
            axis=1
        )

        # Pivot column: identify direct UMLS CUI dbxrefs.
        # Do not sort by dbxref.
        df_direct_umls = (
            df_by_dbxref[df_by_dbxref['dbxrefs'].str.lower().str.startswith('umls:')]
            .groupby('node_id', sort=False)['dbxrefs']
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
                                                left_on='dbxrefs',
                                                right_on='properties_codeid')
                              .rename(columns={'label_x': 'label'}))
        # Build a list of CUIs, grouped by the dbxref code.
        transitive_map = (
            df_transitive_umls.groupby('dbxrefs')['start_id']
            .apply(lambda x: x.dropna().unique().tolist())
        )
        # Pivot column: list of transitive CUIs.
        df_by_dbxref['transitive_cuis'] = df_by_dbxref['dbxrefs'].map(transitive_map)

        # Pivot column: identify a CUI for the code.
        df_by_dbxref['self_cui'] = self._mint_new_CUI(code=df_by_dbxref['node_id'])

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

    def _mint_new_CUI(self, code:Any) -> str:
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

    def _get_CUIs_for_nodes(self, df_nodes: pd.DataFrame) -> pd.Series:
        """
        Vectorized version of _get_CUI_for_node.
        Processes all nodes at once instead of row by row.

        Identifies the CUIs for the concepts to which to link
        JKGEN nodes, based on a ranked evaluation of the cross-references
        (dbxrefs) for each JKGEN node.

        ---
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
        1. the first UMLS CUI
        2. the first other CUI
        3. a CUI minted from the code

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
                
                node_id dbxrefs
                node1   SAB1:Code1|SAB2:Code2
                
                to:
                node_id dbxrefs
                node1   [SAB1:Code1, SAB2:Code2]
                
                to:
                node_id dbxrefs
                node1   SAB1:Code1
                node1   SAB2:Code2
        """

        df_nodes = df_nodes.copy()
        # 1.a, 1.b
        df_nodes['dbxrefs'] = (df_nodes['dbxrefs'].fillna('').str.split('|'))
        # 1.c
        df_exploded = df_nodes[['node_id', 'dbxrefs']].explode('dbxrefs').reset_index(drop=True)
        # 1.d
        df_exploded = df_exploded[df_exploded['dbxrefs'].str.strip() != '']

        """
        2. Standardize dbxref codes, grouping by SAB. 
           (The standardization of a code depends on the code SAB.)
           a. Extract SABs from dbxref codes.
           b. Convert each dbxref into a series to standardize the code.
           c. Group standardized codes by SAB.
           d. Collect standardized codes into lists.
        """
        # 2a.
        df_exploded['sab'] = df_exploded['dbxrefs'].str.split(':').str[0]
        # 2b-d.
        df_exploded['dbxrefs'] = pd.concat([
            pd.Series(
                self.ustand.standardize_code(x=group['dbxrefs'], sab=sab_val).tolist(),
                index=group.index
            )
            for sab_val, group in df_exploded.groupby('sab')
        ])

        """
        3. Identify UMLS CUIs--dbxrefs that start with 'umls:'.
           a. Filter to only those dbxrefs that start with UMLS.
           b. Group by node_id and collect UMLS CUIs into lists.
        """
        # 3a.
        df_umls = df_exploded[df_exploded['dbxrefs'].str.lower().str.startswith('umls:')]
        # 3b.
        umls_map = (
            df_umls.groupby('node_id', sort=False)['dbxrefs']
            .apply(list)
        )

        """
        4. Identify "other CUIs"--dbxrefs with codes that have CUIs in coderels.
           a. Merge against coderels.
           b. Group by node_id and collect CUIs into lists.
        """
        # 4a.
        df_other = (df_exploded.merge(self.jkg_json_coderels,
                                          how='left',
                                          left_on='dbxrefs',
                                          right_on='properties_codeid').
                    rename(columns={'label_x': 'label'}))
        # 4b.
        other_map = (
            df_other.groupby('node_id')['start_id']
            .apply(lambda x: x.dropna().unique().tolist())
        )

        """
        5. Select CUI per node_id. Select the first CUI from lists 
           in order of:
           1. UMLS CUIs
           2. other CUIs
           If no CUI identified, mint a new CUI.
        """

        def pick_cui(node_id):
            direct = umls_map.get(node_id, [])
            other = other_map.get(node_id, [])
            if direct:
                return direct[0].upper()
            elif other:
                return other[0].upper()
            else:
                return self._mint_new_CUI(node_id)

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
        dfjkgen_new_nodes = ((dfjkgen_nodes.merge(self.jkg_json_coderels,
                                                  how='left',
                                                  left_on='node_id',
                                                  right_on='properties_codeid',
                                                  indicator=True)
                              .query('_merge == "left_only"'))
                             .drop(columns=['_merge', 'properties_codeid'])
                             .rename(columns={'label_x': 'label'}))


        # Filter out nodes that only have node_id in the node file.
        dfjkgen_new_nodes = dfjkgen_new_nodes[
            dfjkgen_new_nodes['label'].notna() &
            (dfjkgen_new_nodes['label'].str.strip() != '')
            ]

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

        list_new_node_concepts = []
        list_new_node_terms = []
        list_new_node_coderels = []

        # Keep track of the concept CUI for nodes.
        # For the case of multiple nodes from a SAB
        # mapped to the same CUI, only the first node from
        # the SAB links to the CUI.
        cui = 'start'

        # Identify CUIs to which to assign new nodes.
        dfjkgen_new_nodes['cui'] = self._get_CUIs_for_nodes(df_nodes=dfjkgen_new_nodes)

        for index, row in tqdm(dfjkgen_new_nodes.iterrows(), total=dfjkgen_new_nodes.shape[0]):

            # CUI for new node's Concept, based on dbxref.
            new_node_cui = row['cui']
            if new_node_cui == cui:
                # This is a subsequent node from the SAB that shares a CUI.
                # Mint a new CUI.
                new_node_cui = self._mint_new_CUI(code=row['node_id'])
            else:
                # This is the first node from the SAB that shares a CUI.
                cui = new_node_cui

            # If the CUI is new, create a new Concept label node.
            if new_node_cui == self._mint_new_CUI(code=row['node_id']):
                list_new_node_concepts.append(self._new_node_concept(cui=new_node_cui, row=row))

            # Term label nodes for the new node and its synonyms
            list_new_node_terms = self._new_node_terms(row=row)

            # List of coderels for the node.
            list_new_node_coderels = self._new_node_coderels(cui=new_node_cui, row=row)

            # Collect nodes and rels.
            self.new_jkg_json_nodes = (self.new_jkg_json_nodes
                                       + list_new_node_concepts
                                       + list_new_node_terms)

            self.new_jkg_json_coderels = (self.new_jkg_json_coderels
                                          + list_new_node_coderels)


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
        2. Merge against new node codes to obtain new CUIs for JKGEN subject
           nodes.
        """
        # 1
        dfsubject = self.jkgen.edges.copy()
        dfsubject = (dfsubject.merge(self.jkg_json_coderels,
                                    how='left',
                                    left_on='subject',
                                    right_on='properties_codeid')
                     .rename(columns={'label_x': 'label'}))
        # 2
        dfsubject = (dfsubject.merge(self.new_jkg_json_coderels,
                                    how='left',
                                    left_on='subject_cui',
                                    right_on='properties_codeid')
                     .rename(columns={'label_x': 'label'}))
        """
        Identify the object node CUI.
        1. Merge JKGEN object node codes against JKGJSON coderel codes
           to obtain existing CUIs for JKGEN object nodes.
        2. Merge against new node codes to obtain new CUIs for JKGEN object
           nodes.
        """
        # 3
        dfobject = self.jkgen.edges.copy()
        dfobject = (dfobject.merge(self.jkg_json_coderels,
                                     how='left',
                                     left_on='object',
                                     right_on='properties_codeid')
                     .rename(columns={'label_x': 'label'}))
        # 2
        dfobject = (dfobject.merge(self.new_jkg_json_coderels,
                                     how='left',
                                     left_on='object',
                                     right_on='properties_codeid')
                     .rename(columns={'label_x': 'label'}))

        # Standardize the predicate's relationship label.
        predicate = self.jkgen.edges['predicate']
        self.jkgen.edges['predicate_standardized'] = self.ustand.standardize_relationships(predicate=predicate)

        listrels = []

        for index, row in tqdm(self.jkgen.edgess.iterrows(), total=self.jkgen.edges.shape[0]):
            rel= {
                "labels": row['predicate_standardized'],
                "start_id": row['subject_cui'],
                "end_id": row['object_cui'],
                "properties_sab": self.sab
            }
            listrels.append(rel)

        self.jkg_json_rels = listrels

    def _write_new_jkgjson_arrays(self):

        # Write out nodes and rels arrays for the new JKGEN nodes.

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
