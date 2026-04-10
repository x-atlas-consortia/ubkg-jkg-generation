"""
ubkg_standardizer.py
Class that standardizes formats of content ingested into the UBKG-JKG,
including:
- code ids for nodes
- relationship labels for relationships
- terms
"""
import numpy as np
import pandas as pd
import os

# Centralized logging
from .ubkg_logging import ubkgLogging

class ubkgStandardizer:

    def __init__(self, ulog: ubkgLogging, repo_root: str):

        """

        :param ulog: logger object
        :param repo_root: root of the repository, for absolute file paths

        """
        self.ulog = ulog
        self.repo_root = repo_root

        # Special prefix to SAB maps for code standardization
        self.prefix_sab_maps = self._getprefix_sab_maps()

        # Relationship triples from Relations Ontology for relationship standardization.
        self.relationshiptriples = self._get_relationshiptriples_from_ro()

    def _getprefix_sab_maps(self) -> pd.DataFrame:
        """
        A number of UBKG sources use idiosyncratic naming conventions
        for code identifiers. These cases are mapped manually to SABs
        in a resource file (CSV) that maps prefixes to SABs.
        :return: Pandas DataFrame
        """

        # Find absolute path to file.
        file = 'prefix_sab_map.csv'
        fpath = os.path.join(self.repo_root, 'generation_framework/utilities', file)
        df = pd.DataFrame()

        if os.path.exists(fpath):
            self.ulog.print_and_logger_info(f"Using prefix-sab map file at {fpath}")
            df = pd.read_csv(fpath)
        else:
            # Using print here instead of logging to allow functioning with parsetester.py.
            self.ulog.print_and_logger_error(f'Missing prefix-sab map file {fpath}')
            exit(1)
        return df

    def standardize_code(self, x: pd.Series, ingestSAB: str) -> pd.Series:

        """
        Converts strings that correspond to either codes or CUIs for concepts to a standard format
        for the UBKG.

        The standard format is SAB:code.

        :param x: Pandas Series containing information on either:
                  a set of nodes (subject or object)
                  a set of dbxrefs (cross-reference for a node)
        :param ingestSAB: the SAB for a set of assertions that is being ingested into the UBKG.
        :return:
        """

        # APRIL 2026 - MAY NO LONGER BE THE CASE WITH JKG.
        # JULY 2023 -
        # 1. Assume that data from SABs from UMLS have been reformatted so that
        #    HGNC HGNC:CODE -> HGNC CODE
        #    GO GO:CODE -> GO CODE
        #    HPO HP:CODE -> HPO CODE
        # 2. Establishes the colon as the exclusive delimiter between SAB and code.
        # -------

        # Because of the variety of formats used for codes in various sources, this standardization is
        # complicated.

        # For the majority of nodes, especially those from either UMLS or from OBO-compliant OWL files in RDF/XML
        # serialization,
        # the formatting is straightforward. However, there are a number of special cases, which are handled below.

        # For some SABs, the reformatting is complicated enough to warrant a resource file named prefixes.csv, which
        # should be in the application folder.

        # ---------------
        # DEFAULT
        # Convert the code string to the CodeID format.
        # The colon, underscore, and space characters are reserved as delimiters between SAB and code in input sources--e.g.,
        #   SAB:CODE
        #   SAB_CODE
        #   SAB CODE
        # However, the underscore is also used in code strings in some cases--e.g., RefSeq, with REFSEQ:NR_number.

        # In addition, the hash and backslash figure are delimiters in URIs--e.g., ...#/SAB_CODE

        # Start by reformatting as SAB<space>CODE. The exclusive delimiter (colon) will be added at the end of this
        # script.
        ret = x.str.replace(':', ' ').str.replace('#', ' ').str.replace('_', ' ').str.split('/').str[-1]

        # --------------
        # SPECIAL CONVERSIONS: UMLS SABS
        # 1. Standardize SABs--e.g., convert NCBITaxon (from IRIs) to NCBI (in UMLS); MESH to MSH; etc.
        # 2. For various reasons, some SABs in the UMLS diverge from the standard format.
        #    A common divergence is the case in which the SAB is included in the code to account
        #    for codes with leading zeroes
        #    -- e.g., HGNC, GO, HPO

        # July 2023 - replaced space with colon for delimiter.

        # NCI Thesaurus
        ret = ret.str.replace('NCIT ', 'NCI:', regex=False)
        # MESH
        ret = ret.str.replace('MESH ', 'MSH:', regex=False)

        # NCBI Taxonomy
        ret = ret.str.replace('NCBITaxon ', 'NCBI:', regex=False)
        # UMLS
        ret = ret.str.replace('.*UMLS.*\s', 'UMLS:', regex=True)
        # SNOMED
        ret = ret.str.replace('.*SNOMED.*\s', 'SNOMEDCT_US:', regex=True)

        # FMA
        ret = ret.str.replace('^fma', 'FMA:', regex=True)

        # HGNC
        # Note that non-UMLS sets of assertions may also refer to HGNC codes differently. See below.
        ret = ret.str.replace('Hugo.owl HGNC ', 'HGNC:', regex=False)

        # Deprecated July 2023; the incoming format is now HGNC:code.
        # ret = ret.str.replace('HGNC ', 'HGNC HGNC:', regex=False)
        # Changed July 2023
        # ret = ret.str.replace('gene symbol report?hgnc id=', 'HGNC HGNC:', regex=False)
        ret = ret.str.replace('gene symbol report?hgnc id=', 'HGNC:', regex=False)

        # -------------
        # SPECIAL CASES - Non-UMLS sets of assertions

        # Ontologies such as HRAVS refer to NCI Thesaurus nodes by IRI.
        ret = np.where(x.str.contains('Thesaurus.owl'), 'NCI:' + x.str.split('#').str[-1], ret)

        # UNIPROTKB
        # The HGNC codes in the UNIPROTKB ingest files were in the expected format of HGNC HGNC:code.
        # Remove duplications introduced from earlier conversions in this script.
        # Deprecated July 2023: incoming format is now HGNC:code.
        # ret = np.where(x.str.contains('HGNC HGNC:'), x, ret)

        # EDAM
        # EDAM uses subdomains--e.g, format_3750, which translates to a SAB of "format". Force all
        # EDAM nodes to be in a SAB named EDAM.

        # EDAM has two cases:
        # 1. When obtained from edge file for source or object nodes, EDAM IRIs are in the format
        #    http://edamontology.org/<domain>_<id>
        #    e.g., http://edamontology.org/format_3750
        # 2. When obtained from node file for dbxref, EDAM codes are in the format
        #    EDAM:<domain>_<id>

        # Case 2 (dbxref)
        ret = np.where((x.str.contains('EDAM')), x.str.split(':').str[-1], ret)
        # Case 1 (subject or object node)
        ret = np.where((x.str.contains('edam')), 'EDAM:' + x.str.replace(' ', '_').str.split('/').str[-1], ret)

        # MONDO
        # Two cases to handle:
        # 1. MONDO identifies genes with IRIs in format
        # http://identifiers.org/hgnc/<id>
        # Convert to HGNC HGNC:<id>
        # Changed July 2023
        # ret = np.where(x.str.contains('http://identifiers.org/hgnc'),
                       #'HGNC HGNC:' + x.str.split('/').str[-1], ret)
        ret = np.where(x.str.contains('http://identifiers.org/hgnc'),
                       'HGNC:' + x.str.split('/').str[-1], ret)
        # 2. MONDO uses both OBO-3 compliant IRIs (e.g., "http://purl.obolibrary.org/obo/MONDO_0019052") and
        #    non-compliant ones (e.g., "http://purl.obolibrary.org/obo/mondo#ordo_clinical_subtype")
        ret = np.where(x.str.contains('http://purl.obolibrary.org/obo/mondo#'),
                       'MONDO:' + x.str.split('#').str[-1], ret)

        # MAY 2023
        # PGO
        # Restore changes made related to GO.
        # PGO nodes are written as http://purl.obolibrary.org/obo/PGO_(code)
        # Deprecated July 2023
        # ret = np.where(x.str.contains('PGO'),
                       # 'PGO PGO:' + x.str.split('_').str[-1], ret)

        # REFSEQ - restore underscore between NR and number.
        # JULY 2023 - Refactored for SAB:CODE refactoring
        # Assumes that code at this point is in format REFSEQ NR X, to be reformatted as REFSEQ:NR_X.
        ret = np.where(x.str.contains('REFSEQ'), x.str.replace('REFSEQ ', 'REFSEQ:').str.replace(' ', '_'), ret)

        # July 2023
        # MSIGDB - restore underscores.
        ret = np.where(x.str.contains('MSIGDB'), x.str.replace('MSIGDB ', 'MSIGDB:').str.replace(' ', '_'), ret)

        # January 2025
        # REACTOME - restore underscores.
        ret = np.where(x.str.contains('REACTOME'), x.str.replace('REACTOME ', 'REACTOME:').str.replace(' ', '_'), ret)


        # MAY 2023
        # HPO
        # If expected format (HPO HP:code) was used, revert to avoid duplication.
        # Deprecated July 2023: incoming code format now HPO:CODE.
        # ret = np.where(x.str.contains('HPO HP:'),
                        #'HPO HP:' + x.str.split(':').str[-1], ret)

        # HCOP
        # The HCOP node_ids are formatted to resemble HGNC node_ids.
        # Deprecated July 2023; no longer needed because HGNC is now formatted as HGNC:CODE.
        # ret = np.where(x.str.contains('HCOP'),'HCOP HCOP:' + x.str.split(':').str[-1],ret)

        # SEPT 2023
        # CEDAR
        ret = np.where(x.str.contains('https://repo.metadatacenter.org/templates/'),
                       'CEDAR:' + x.str.split('/').str[-1], ret)
        ret = np.where(x.str.contains('https://repo.metadatacenter.org/template-fields/'),
                       'CEDAR:' + x.str.split('/').str[-1], ret)
        ret = np.where(x.str.contains('https://schema.metadatacenter.org/core/'),
                       'CEDAR:' + x.str.split('/').str[-1], ret)
        ret = np.where(x.str.contains('http://www.w3.org/2001/XMLSchema'),
                       'XSD:' + x.str.split('#').str[-1], ret)

        # HRAVS
        # The HRAVS IRIs are in format ...hravs#HRAVS_X, which results in HRAVS HRAVS X.
        ret = np.where(x.str.contains('https://purl.humanatlas.io/vocab/hravs#'),
                       'HRAVS:' + x.str.split('_').str[-1], ret)
        # The gzip_csv converter script translates HRAVS IRIs to hravs HRAVS X.
        ret = np.where(x.str.upper().str.contains('HRAVS HRAVS'),
                       'HRAVS:' + x.str.split(' ').str[-1], ret)

        # ORDO
        # ORDO uses Orphanet as a namespace.
        ret = np.where(x.str.contains('http://www.orpha.net/ORDO/'),
                       'ORDO:' + x.str.split('_').str[-1], ret)


        # PREFIX-SAB maps
        # A number of ontologies, especially those that originate from Turtle files, use prefixes that are
        # translated to IRIs that are not formatted as expected.
        # Map IRIs to SABs using prefix-sab map file.
        for index, row in self.prefix_sab_maps.iterrows():
            if ingestSAB in ['GLYCOCOO', 'GLYCORDF']:
                # GlyCoCOO (a Turtle) and GlyCoRDF use IRIs that delimit with hash and use underlines.
                # "http://purl.glycoinfo.org/ontology/codao#Compound_disease_association
                # July 2023 - refactored to use colon as SAB:code delimiter
                ret = np.where(x.str.contains(row['prefix']), row['SAB'] + ':' +
                               x.str.replace(' ', '_').str.replace('/', '_').str.split('#').str[-1],
                               ret)
            # July 2023: other prefixes are only from NPO, NPOSKCAN
            else:
                if ingestSAB in ['NPO', 'NPOSKCAN']:
                    # Other SABs format IRIs with a terminal backslash and the code string.
                    # A notable exception is the PantherDB format (in NPOSKCAN), for which the IRI is an API call
                    # (e.g., http://www.pantherdb.org/panther/family.do?clsAccession=PTHR10558).
                    # July 2023 - refactored to use colon as SAB:code delimiter
                    ret = np.where(x.str.contains(row['prefix']), row['SAB'] + ':' +
                                   x.str.replace(' ', '_').str.replace('/', '_').str.replace('=', '_').str.split('_').str[-1],
                                   ret)

        # UNIPROT (not to be confused with UNIPROTKB).
        # UNIPROT IRIs are formatted differently than those in Glygen, but are in the Glygen OWL files, so they need
        # to be translated separately from GlyGen nodes.
        # July 2023 - refactored to use colon as SAB:code delimiter
        ret = np.where(x.str.contains('uniprot.org'), 'UNIPROT:' + x.str.split('/').str[-1], ret)

        # July 2023 - For Data Distillery use cases, where code formats conformed to the earlier paradigms.
        # HGNC HGNC:
        # HPO HP:
        # HCOP HCOP:
        ret = np.where(x.str.contains('HGNC HGNC:'), x.str.replace('HGNC HGNC:', 'HGNC:'), ret)
        # January 2024 - standardized to HP from HPO.
        ret = np.where(x.str.contains('HPO HP:'), x.str.replace('HPO HP:', 'HP:'), ret)
        ret = np.where(x.str.contains('HCOP HCOP:'), x.str.replace('HCOP HCOP:', 'HCOP:'), ret)

        ret = np.where(x.str.contains('NCBI Gene'), x.str.replace('NCBI Gene', 'ENTREZ:'), ret)

        # JANUARY 2024 - GENCODE_VS
        # Restore the underscore.
        ret = np.where(x.str.contains('GENCODE_VS'),x.str.replace('GENCODE:VS', 'GENCODE_VS'), ret)

        # AUGUST 2025 - SENOTYPE_VS
        # Restore the underscore.
        ret = np.where(x.str.contains('SENOTYPE_VS'), x.str.replace('SENOTYPE:VS', 'SENOTYPE_VS'), ret)

        # ---------------
        # FINAL PROCESSING

        # At this point in the script, the code should be in one of two formats:
        # 1. SAB CODE, where
        #    a. SAB may be lowercase
        #    b. CODE may be mixed case, wiht spaces.
        # 2. The result of a custom formatting--e.g., HGNC:code.

        # The assumption is that if there are spaces at this point, the first space is the one between the SAB
        # and the code.

        # Force SAB to uppercase. Force the colon to be the delimiter between SAB and code.

        # After the preceding conversions, ret has changed from a Pandas Series to a numpy array.
        # 1. Split each element on the initial space, if one exists.
        # 2. Convert the SAB portion (first element) to uppercase.
        # 3. Add the colon between the SAB (first element) and the code.

        # Note: Special cases should already be in the correct code format of SAB:code.

        for idx, x in np.ndenumerate(ret):
            xsplit = x.split(sep=' ', maxsplit=1)
            if len(xsplit) > 1:
                sab = xsplit[0].upper()
                code = ' '.join(xsplit[1:len(xsplit)])
                ret[idx] = sab+':'+code
            elif len(x)>0:
                # JULY 2023
                # For the case of a CodeID that appears to be a "naked" UMLS CUI, format as UMLS:CUI.
                # SEPT 2023 - Account for codes in the CEDAR SAB.
                if x[0] == 'C' and not 'CEDAR' in x and x[1].isnumeric:
                 ret[idx] = 'UMLS:'+x
            else:
                ret[idx] = x

        return ret

    def identify_relationships(self, predicate: pd.Series) -> pd.Series:
        """
        1. Checks relationships in an edge file
        against the Relations Ontology.
        2. Standardizes relationship strings for use in the UBKG JKG.

        :param predicate: Pandas Series object containing predicates (edges):
        :return:
        """

        # Perform a series of joins and rename resulting columns to keep track of the source of
        # relationship labels and inverse relationship labels.

        # Convert input to DataFrame for merge operations.
        df = predicate.to_frame()

        # Check for relationships in RO, considering the edgelist predicate as a *full IRI*.
        df = df.merge(self.relationshiptriples, how='left', left_on='predicate',
                                  right_on='IRI').drop_duplicates().reset_index(drop=True)
        #df = df[
            #['predicate' 'relation_label_RO',
             #'inverse_RO']].rename(
            #columns={'relation_label_RO': 'relation_label_RO_fromIRIjoin', 'inverse_RO': 'inverse_RO_fromIRIjoin'})

        print(df)

    def standardize_relationship(self,x: pd.Series) -> pd.Series:

        """
        # Converts strings that correspond to a predicate string to a format recognized by the generation
        # framework
        :param x: Pandas Series object containing predicates (edges)
        :return: Pandas Series object of standardized predicates.
        """


        # Final formatting

        # 1. Replace . and - with _
        # 2. Format relationship strings to comply with neo4j naming rules:
        #    a. Only alphanumeric characters or the underscore.
        #    b. Prepend "rel_" to relationships with labels that start with numbers.

        ret = x.str.replace('.', '_', regex=False)
        ret = ret.str.replace('-', '_', regex=False)
        ret = ret.str.replace('(', '_', regex=False)
        ret = ret.str.replace(')', '_', regex=False)
        ret = ret.str.replace('[', '_', regex=False)
        ret = ret.str.replace(']', '_', regex=False)
        ret = ret.str.replace('{', '_', regex=False)
        ret = ret.str.replace('}', '_', regex=False)
        ret = ret.str.replace(':', '_', regex=False)

        ret = ret.str.lower()
        ret = np.where(ret.astype(str).str[0].str.isnumeric(),'rel_' + ret, ret)


        # For the majority of edges, especially those from either UMLS or from OBO-compliant
        # OWL files in RDF/XML serialization,
        # the format of an edge is one of the following:
        # 1. A IRI in the form http://purl.obolibrary.org/obo/RO_code
        # 2. RO_code
        # 3. RO:code
        # 4. a string

        # Predicates are in lowercase.
        ret = np.where(x.str.contains('ro:'), 'http://purl.obolibrary.org/obo/ro_' + x.str.split('ro:').str[-1], ret)

        # Replace #type from RDF schemas with isa.
        ret = np.where(x.str.contains('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), 'isa', ret)
        ret = np.where(x.str.contains('http://www.w3.org/2000/01/rdf-schema#type'), 'isa', ret)



        return ret

    def parse_string_nested_parentheses(self,strparen: str) -> list[tuple]:

        """
        Analyzes a string with nested parentheses in terms of level of nesting.

        For example, '(a(b(c)(d)e)(f)g)' can be analyzed as:
        level 0: a(b(c)(d)e)(f)g
        level 1: f
        level 1: b(c)(d)e
        level 2: c
        level 2: d
        or
        [(2, 'c'), (2, 'd'), (1, 'b(c)(d)e'), (1, 'f'), (0, 'a(b(c)(d)e)(f)g')]

        UBKG use case: UniprotKB, which uses parentheses both as delimiters and
        inside elements--e.g., (element 1 (details))(element 2 (details))

        Adapted from a solution posted by Gareth Rees at
        https://stackoverflow.com/questions/4284991/parsing-nested-parentheses-in-python-grab-content-by-level

        """
        return list(self.parenthetic_contents(strparen))

    def parenthetic_contents(self,strparen: str) -> tuple:

        # Employs a stack to analyze elements in a string by level of nesting.
        stack = []
        for i, c in enumerate(strparen):
            if c == '(':
                # New level of parenthesis nesting.
                stack.append(i)
            elif c == ')' and stack:
                # Closing of element at this level of nesting.
                # Return to higher level of nesting.
                start = stack.pop()
                yield len(stack), strparen[start + 1: i]

    def _get_relationshiptriples_from_ro(self) -> pd.DataFrame:

        """
        Obtains a set of relationship triples from the Relations Ontology (RO).
        :return: DataFrame
        """

        """
        Obtain descriptions of relationships and their inverses from the Relations Ontology JSON.

        The Relations Ontology (RO) is an ontology of relationships, in which the nodes are
        relationship properties and the edges (predicates) are relationships *between*
        relationship properties.
        For example,
        
        relationship property RO_0002292 (node) inverseOf (edge) relationship property RO_0002206 (node)
        or
        
        "expresses" inverseOf "expressed in"
        
        """

        self.ulog.print_and_logger_info(' * Obtaining relationship reference information from Relations Ontology...')
        # Fetch the RO JSON.
        dfro = pd.read_json("https://raw.githubusercontent.com/oborel/obo-relations/master/ro.json")

        # Information on relationship properties (i.e., relationship property nodes) is in the node array.
        dfrelnodes = pd.DataFrame(dfro.graphs[0]['nodes'])

        # Information on edges (i.e., relationships between relationship properties) is in the edges array.
        dfreledges = pd.DataFrame(dfro.graphs[0]['edges'])

        """
        Information on the relationships between relationship properties *should be* in the edges array.
        Example of edge element:
        {
          "sub" : "http://purl.obolibrary.org/obo/RO_0002101",
          "pred" : "inverseOf",
          "obj" : "http://purl.obolibrary.org/obo/RO_0002132"
        }
        
        Not all relationships in RO are defined with inverses; for these relationships, the script can define
        "pseudo-inverse" relationships--e.g., if the only information available is the label "eats", then
        the pseudo-inverse will be "inverse_eats" (instead of, say, "eaten_by").

        Cases that require pseudo-inverses include:
        1. A property is incompletely specified in terms of both sides of an inverse relationship--e.g.,
           RO_0002206 is listed as the inverse of RO_0002292, but RO_0002292 is not listed as the
           corresponding inverse of RO_0002206. For these properties, the available relationship
           will be inverted when joining relationship information to the edgelist.
           (This is really a case in which both directions of the inverse relationship should have been
           defined in the edges node, but were not.)
           
        2. A property does not have inverse relationships defined in RO.
           The relationship will be added to the list with a null inverse. The script will later create a
           pseudo-inverse by appending "inverse_" to the relationship label.
        """

        # Obtain triple information for relationship properties--i.e.,
        # 1. IRIs for "subject" nodes and "object" nodes (relationship properties)
        # 2. relationship predicates (relationships between relationship properties)

        # Get subject node, edge
        dfrt = dfrelnodes.merge(dfreledges, how='left', left_on='id', right_on='sub')
        # Get object node
        dfrt = dfrt.merge(dfrelnodes, how='left', left_on='obj', right_on='id')

        # Set a default predicate to capture nodes without predicates.
        dfrt = dfrt.fillna(value={'pred': 'no predicate'})

        # ---------------------------------
        # Identify relationship properties that do not have inverses.
        # 1. Group relationship properties by predicate, using count.
        #    ('pred' here describes the relationship between relationship properties.)
        dfpred = dfrt.groupby(['id_x', 'pred']).count().reset_index()

        # 2. Identify the relationships for which the set of predicates does not include "inverseOf".
        listinv = dfpred[dfpred['pred'] == 'inverseOf']['id_x'].to_list()
        listnoinv = dfpred[~dfpred['id_x'].isin(listinv)]['id_x'].to_list()
        dfnoinv = dfrt.copy()
        dfnoinv = dfnoinv[dfnoinv['id_x'].isin(listnoinv)]

        # 3. Rename column names to match the relationtriples frame. (Column names are described
        #    farther down.)
        dfnoinv = dfnoinv[['id_x', 'lbl_x', 'id_y', 'lbl_y']].rename(
            columns={'id_x': 'IRI', 'lbl_x': 'relation_label_RO', 'id_y': 'inverse_IRI', 'lbl_y': 'inverse_RO'})
        # The inverses are undefined.
        dfnoinv['inverse_IRI'] = np.nan
        dfnoinv['inverse_RO'] = np.nan

        # ---------------------------------
        # Look for members of incomplete inverse pairs--i.e., relationship properties that are
        # the *object* of an inverseOf edge, but not the corresponding *subject* of an inverseOf edge.
        #
        # 1. Filter edges to inverseOf.
        dfedgeinv = dfreledges[dfreledges['pred'] == 'inverseOf']

        # 2. Find all relation properties that are objects of inverseOf edges.
        dfnoinv = dfnoinv.merge(dfedgeinv, how='left', left_on='IRI', right_on='obj')

        # 3. Get the label for the relation properties that are subjects of inverseOf edges.
        dfnoinv = dfnoinv.merge(dfrelnodes, how='left', left_on='sub', right_on='id')
        dfnoinv['inverse_IRI'] = np.where(dfnoinv['lbl'].isnull(), dfnoinv['inverse_IRI'], dfnoinv['id'])
        dfnoinv['inverse_RO'] = np.where(dfnoinv['lbl'].isnull(), dfnoinv['inverse_RO'], dfnoinv['lbl'])
        dfnoinv = dfnoinv[['IRI', 'relation_label_RO', 'inverse_IRI', 'inverse_RO']]

        # ---------------------------------
        # Filter the base triples frame to just those relationship properties that have inverses.
        # This step eliminates relationship properties related by relationships such as "subPropertyOf".
        dfrt = dfrt[dfrt['pred'] == 'inverseOf']

        # Rename column names.
        # Column names will be:
        # IRI - the IRI for the relationship property
        # relation_label_RO - the label for the relationship property
        # inverse_RO - the label of the inverse relationship property
        # inverse_IRI - IRI for the inverse relationship (This will be dropped.)
        dfrt = dfrt[['id_x', 'lbl_x', 'id_y', 'lbl_y']].rename(
            columns={'id_x': 'IRI', 'lbl_x': 'relation_label_RO', 'id_y': 'inverse_IRI', 'lbl_y': 'inverse_RO'})

        # Add triples for problematic relationship properties--i.e., without inverses or from incomplete pairs.
        dfrt = pd.concat([dfrt, dfnoinv], ignore_index=True).drop_duplicates(subset=['IRI'])

        # Convert stings for labels for relationships to expected delimiting.
        dfrt['relation_label_RO'] = \
            dfrt['relation_label_RO'].str.replace(' ', '_').str.split('/').str[-1]
        dfrt['inverse_RO'] = dfrt['inverse_RO'].str.replace(' ', '_').str.split('/').str[-1]

        dfrt = dfrt.drop(columns='inverse_IRI')

        # Cast IRI to lowercase.
        dfrt['IRI'] = dfrt['IRI'].str.lower()

        return dfrt