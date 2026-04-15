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

# Timer for block actions
from .ubkg_timer import UbkgTimer

class ubkgStandardizer:

    def __init__(self, ulog: ubkgLogging, repo_root: str):

        """

        :param ulog: logger object
        :param repo_root: root of the repository, for absolute file paths

        """
        self.ulog = ulog
        self.repo_root = repo_root

        # Special prefix to SAB maps for code standardization
        self.prefix_sab_maps = self._get_prefix_sab_maps()

    def _get_rel_label_maps(self) -> pd.DataFrame:
        """
        A number of edges in UBKG OWL sources use codes that are not
        specified in the Relationship Ontology, but that can be
        obtained from other sources, such as OBO.

        References:
        1. OBI: https://dashboard.obofoundry.org/dashboard/obi/fp7.html
        2.

        These edges are mapped in a CSV file.
        :return: Pandas DataFrame
        """

        # Find absolute path to file.
        file = 'rel_label_maps.csv'
        fpath = os.path.join(self.repo_root, 'generation_framework/utilities', file)
        df = pd.DataFrame()

        if os.path.exists(fpath):
            self.ulog.print_and_logger_info(f"Using relationship-label map file at {fpath}")
            df = pd.read_csv(fpath)
        else:
            # Using print here instead of logging to allow functioning with parsetester.py.
            self.ulog.print_and_logger_error(f'Missing relationship-label map file {fpath}')
            exit(1)
        return df

    def _get_prefix_sab_maps(self) -> pd.DataFrame:
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

    def standardize_code(self, x: pd.Series, sab: str) -> pd.Series:

        """
        Converts strings that correspond to either codes or CUIs for concepts to a standard format
        for the UBKG.

        The standard format is SAB:code.

        :param x: Pandas Series containing information on either:
                  a set of nodes (subject or object)
                  a set of dbxrefs (cross-reference for a node)
        :param sab: the SAB for a set of assertions that is being ingested into the UBKG.
        :return:
        """

        """
        Because of the variety of formats used for codes in various sources, this standardization is
        complicated.

        For the majority of nodes, especially those from either UMLS or from OBO-compliant OWL files in RDF/XML
        serialization,
        the formatting is straightforward. However, there are a number of special cases, which are handled below.

        For some SABs, the reformatting is complicated enough to warrant a resource file of prefix maps, which
        should be in the utilities folder.

        1. Assume that data from SABs from UMLS have been reformatted so that
           HGNC HGNC:CODE -> HGNC CODE
           GO GO:CODE -> GO CODE
           HPO HP:CODE -> HPO CODE
        2. The colon is the exclusive delimiter between SAB and code.
        
        """

        """
        DEFAULT
        Convert the code string to the CodeID format.
        The colon, underscore, and space characters are reserved as delimiters between SAB and code in input sources--e.g.,
          SAB:CODE
          SAB_CODE
          SAB CODE
          
        However, the underscore is also used in code strings in some cases--e.g., RefSeq, with REFSEQ:NR_number.

        In addition, the hash and backslash figure are delimiters in URIs--e.g., ...#/SAB_CODE
        """

        utime = UbkgTimer(f'Standardizing codes for {x.name}')

        # Start by reformatting as SAB<space>CODE. The exclusive delimiter (colon) will be added at the end of this
        # script.
        ret = x.str.replace(':', ' ').str.replace('#', ' ').str.replace('_', ' ').str.split('/').str[-1]

        """
        SPECIAL CONVERSIONS: UMLS SABS
        1. Standardize SABs--e.g., convert NCBITaxon (from IRIs) to NCBI (in UMLS); MESH to MSH; etc.
        2. For various reasons, some SABs in the UMLS diverge from the standard format.
           A common divergence is the case in which the SAB is included in the code to account
           for codes with leading zeroes
           -- e.g., HGNC, GO, HPO
        """

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
        # Note that non-UMLS sets of assertions may also refer to HGNC codes differently.
        # See below.
        ret = ret.str.replace('Hugo.owl HGNC ', 'HGNC:', regex=False)
        ret = ret.str.replace('gene symbol report?hgnc id=', 'HGNC:', regex=False)

        # -----------
        # SPECIAL CASES - Non-UMLS sets of assertions
        # -----------

        # Ontologies such as HRAVS refer to NCI Thesaurus nodes by IRI.
        ret = np.where(x.str.contains('Thesaurus.owl'), 'NCI:' + x.str.split('#').str[-1], ret)

        """
        EDAM
        
        EDAM uses subdomains--e.g, format_3750, which translates to a SAB of "format". 
        Force all EDAM nodes to be in a SAB named EDAM.

        EDAM has two cases:
        1. When obtained from edge file for source or object nodes, EDAM IRIs are in the format
           http://edamontology.org/<domain>_<id>
           e.g., http://edamontology.org/format_3750
        2. When obtained from node file for dbxref, EDAM codes are in the format
           EDAM:<domain>_<id>

        """
        # EDAM Case 2 (dbxref)
        ret = np.where((x.str.contains('EDAM')), x.str.split(':').str[-1], ret)
        # EDAM Case 1 (subject or object node)
        ret = np.where((x.str.contains('edam')), 'EDAM:' + x.str.replace(' ', '_').str.split('/').str[-1], ret)


        # MONDO
        # Two cases to handle:
        # 1. MONDO identifies genes with IRIs in format
        # http://identifiers.org/hgnc/<id>
        # Convert to HGNC HGNC:<id>
        ret = np.where(x.str.contains('http://identifiers.org/hgnc'),
                       'HGNC:' + x.str.split('/').str[-1], ret)
        # 2. MONDO uses both OBO-3 compliant IRIs (e.g., "http://purl.obolibrary.org/obo/MONDO_0019052") and
        #    non-compliant ones (e.g., "http://purl.obolibrary.org/obo/mondo#ordo_clinical_subtype")
        ret = np.where(x.str.contains('http://purl.obolibrary.org/obo/mondo#'),
                       'MONDO:' + x.str.split('#').str[-1], ret)


        # REFSEQ - restore underscore between NR and number.
        # Assumes that code at this point is in format REFSEQ NR X, to be reformatted as REFSEQ:NR_X.
        ret = np.where(x.str.contains('REFSEQ'), x.str.replace('REFSEQ ', 'REFSEQ:').str.replace(' ', '_'), ret)

        # MSIGDB - restore underscores.
        ret = np.where(x.str.contains('MSIGDB'), x.str.replace('MSIGDB ', 'MSIGDB:').str.replace(' ', '_'), ret)

        # REACTOME - restore underscores.
        ret = np.where(x.str.contains('REACTOME'), x.str.replace('REACTOME ', 'REACTOME:').str.replace(' ', '_'), ret)

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
            if sab in ['GLYCOCOO', 'GLYCORDF']:
                # GlyCoCOO (a Turtle) and GlyCoRDF use IRIs that delimit with hash and use underlines.
                # "http://purl.glycoinfo.org/ontology/codao#Compound_disease_association
                # July 2023 - refactored to use colon as SAB:code delimiter
                ret = np.where(x.str.contains(row['prefix']), row['SAB'] + ':' +
                               x.str.replace(' ', '_').str.replace('/', '_').str.split('#').str[-1],
                               ret)
            # July 2023: other prefixes are only from NPO, NPOSKCAN
            else:
                if sab in ['NPO', 'NPOSKCAN']:
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

        """
        FINAL PROCESSING

        At this point in the script, the code should be in one of two formats:
        1. SAB CODE, where
           a. SAB may be lowercase
           b. CODE may be mixed case, wiht spaces.
        2. The result of a custom formatting--e.g., HGNC:code.

        The assumption is that if there are spaces at this point, the first space 
        is the one between the SAB and the code.

        Force SAB to uppercase. 
        Force the colon to be the delimiter between SAB and code.

        After the preceding conversions, ret has changed from a 
        Pandas Series to a numpy array.
        1. Split each element on the initial space, if one exists.
        2. Convert the SAB portion (first element) to uppercase.
        3. Add the colon between the SAB (first element) and the code.

        Note: Special cases should already be in the correct code format of SAB:code.

        """

        for idx, x in np.ndenumerate(ret):
            xsplit = x.split(sep=' ', maxsplit=1)
            if len(xsplit) > 1:
                sab = xsplit[0].upper()
                code = ' '.join(xsplit[1:len(xsplit)])
                ret[idx] = sab+':'+code
            elif len(x)>0:
                # JULY 2023
                # For the case of a CodeID that appears to be a "naked" UMLS CUI, format as UMLS:CUI.
                # Account for codes in the CEDAR SAB.
                if x[0] == 'C' and not 'CEDAR' in x and x[1].isnumeric:
                 ret[idx] = 'UMLS:'+x
            else:
                ret[idx] = x

        utime.stop()
        return ret

    def standardize_relationships(self, predicate: pd.Series) -> pd.Series:
        """
        1. Checks relationships in an edge file
           against references including
           a. Relations Ontology
           b. Biological Spatial Ontology
           c. OBO
        2. Standardizes relationship strings for use in the UBKG JKG.

        :param predicate: Pandas Series object containing predicates (edges):
        :return: Pandas Series object with translated predicate labels

        The Relationship Ontology defines inverse relationships for
        many relationships. The UBKG-JKG only defines forward
        relationships; however, the inverse relationship
        definition logic will be retained.

        """


        # Relationship triples from Relations Ontology for relationship standardization.
        dfro = self._get_relationshiptriples_from_ro()

        # Relationships from the Biological Spatial Ontology
        dfbspo = self._get_relationships_from_bspo()

        # Special code to label maps for relationship standardization
        dfrelmaps = self._get_rel_label_maps()
        utime = UbkgTimer(display_msg='Standardizing relationships')

        # Perform a series of merge and rename resulting columns
        # to keep track of the source of relationship labels
        # and inverse relationship labels (for RO).

        # Convert input to DataFrame for merge operations.
        df = predicate.to_frame()

        """
            A predicate can be one of the following:
            1. a full IRI that has a match to a relationship node in 
               the Relations Ontology
            2. a code for a relationship node in the Relations Ontology
            3. a label that has a match to the label for a 
               relationship node in the Relations Ontology
            4. a label that has a match to the label for a
               relationship node in the Biospatial Spatial Ontology
            5. a label that can be extracted from a #core IRI--e.g.,
               http://purl.obolibrary.org/obo/uberon/core#proximally_connected_to
            6. a custom label
               
       """

        # Check whether the predicate corresponds to a full IRI in RO.
        # RO IRIs are cast to lowercase.
        df['predicate_lower'] = df['predicate'].str.lower()
        df = df.merge(dfro, how='left', left_on='predicate_lower',
                      right_on='IRI').reset_index(drop=True)
        df = (df[[
            'predicate',
            'predicate_lower',
            'relation_label_RO',
            'inverse_label_RO']]
        .rename(columns={
            'relation_label_RO': 'relation_label_RO_from_IRI',
            'inverse_label_RO': 'inverse_label_RO_from_IRI',
        }))

        """
        Check whether the predicate corresponds to a label for a relationship in RO by label.
        First, format the predicate string to match potential relationship strings from RO.
        Parsing note: relationship IRIs often include the '#' character as a terminal delimiter, and
        be in format url...#relation--e.g., ccf.owl#ct_is_a.
        """
        # Strip any URL path prefix.
        df['predicate_label'] = df['predicate'].str.replace(' ', '_').str.replace('#', '/').str.split('/').str[-1]
        df = df.merge(dfro, how='left', left_on='predicate_label',
                      right_on='relation_label_RO').reset_index(drop=True)

        df = (df[[
            'predicate',
            'predicate_lower',
            'relation_label_RO_from_IRI',
            'inverse_label_RO_from_IRI',
            'relation_label_RO',
            'inverse_label_RO']]
        .rename(columns={
            'relation_label_RO': 'relation_label_RO_from_label',
            'inverse_label_RO': 'inverse_label_RO_from_label'
        }))

        """
        Check whether the predicate corresponds to a code in RO, treating the predicate 
        as an abbreviated IRI in format RO:code. (Use case: MPMGI)
        
        """
        # Reformat the predicate string as a full IRI.
        # Replace : with _ and cast to lowercase.
        df['predicate_IRI'] = 'http://purl.obolibrary.org/obo/' + df['predicate'].str.lower().str.replace(':', '_')

        df = df.merge(dfro, how='left', left_on='predicate_IRI',
                      right_on='IRI').reset_index(drop=True)

        df = (df[[
            'predicate',
            'predicate_IRI',
            'predicate_lower',
            'relation_label_RO_from_IRI',
            'inverse_label_RO_from_IRI',
            'relation_label_RO_from_label',
            'inverse_label_RO_from_label',
            'relation_label_RO',
            'inverse_label_RO']]
        .rename(
            columns={
                'relation_label_RO': 'relation_label_RO_from_code',
                'inverse_label_RO': 'inverse_label_RO_from_code'
        }))

        #debug = os.path.join(self.repo_root, 'debug.tsv')
        #df.to_csv(debug, sep='\t', index=False)
        #exit(1)

        """
        Check whether the predicate corresponds to a relationship from BSPO.
        Check on IRI.
        """
        df = df.merge(dfbspo, how='left', left_on='predicate_lower',
                      right_on='id').reset_index(drop=True)
        df = (df[[
            'predicate',
            'relation_label_RO_from_IRI',
            'inverse_label_RO_from_IRI',
            'relation_label_RO_from_label',
            'inverse_label_RO_from_label',
            'relation_label_RO_from_code',
            'inverse_label_RO_from_code',
            'lbl']]
        .rename(
            columns={
                'lbl': 'relation_label_BSPO'
            }))

        """
        Check whether the predicate corresponds to a label that 
        can be extracted from a # IRI.
        """
        df['predicate_#'] = \
        df['predicate'].where(df['predicate'].str.contains('#', na=False)).str.split('#').str[-1]

        # Finally, extract a custom relationship label.
        df['predicate_label'] = df['predicate'].str.split('/').str[-1]


        # Order of precedence for relationship/inverse relationship data:

        # 1. label from the edgelist predicate joined to RO by IRI
        df['relation_label'] = df['relation_label_RO_from_IRI']
        df['inverse_label'] = df['inverse_label_RO_from_IRI']

        # 2. label from the edgelist predicate formatted as RO:code, joined to RO by IRI
        df['relation_label'] = np.where(df['relation_label'].isnull(),
                                              df['relation_label_RO_from_code'],
                                              df['relation_label'])
        df['inverse_label'] = np.where(df['inverse_label'].isnull(),
                                              df['inverse_label_RO_from_code'],
                                              df['inverse_label'])

        # 3. label from the edgelist predicate joined to RO by label
        df['relation_label'] = np.where(df['relation_label'].isnull(),
                                        df['relation_label_RO_from_label'],
                                        df['relation_label'])
        df['inverse_label'] = np.where(df['inverse_label'].isnull(),
                                       df['relation_label_RO_from_label'],
                                       df['inverse_label'])

        # 4. label from the BSPO
        df['relation_label'] = np.where(df['relation_label'].isnull(),
                                        df['relation_label_BSPO'],
                                        df['relation_label'])

        # 5. label extracted from # IRI
        df['relation_label'] = np.where(df['relation_label'].isnull(),
                                        df['predicate_#'],
                                        df['relation_label'])

        # 6. predicate label from edgelist
        df['relation_label'] = np.where(df['relation_label'].isnull(),
                                        df['predicate_label'],
                                        df['relation_label'])

        # Apply custom relationship label replacements.
        # Replace subClassOf with isa.
        df['relation_label'] = np.where(df['relation_label'].str.contains('subClassOf'), 'isa', df['relation_label'])

        # Replace #type from RDF schemas with isa.
        df['relation_label'] = np.where(df['relation_label'].str.contains('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                                        'isa',
                                        df['relation_label'])
        df['relation_label'] = np.where(
            df['relation_label'].str.contains('http://www.w3.org/2000/01/rdf-schema#type'),
            'isa',
            df['relation_label'])

        # Apply custom relationship label maps.
        for _, row in dfrelmaps.iterrows():
            code = row['relationship_code'].strip()
            label = row['relationship_label'].strip()

            # Check for custom relationship code to label map.
            df['relation_label'] = np.where(
                (df['relation_label'].str.strip().str.lower() == code.lower()) |
                (df['relation_label'].str.strip().str.lower().str.endswith(code.lower())),
                label,
                df['relation_label'])

        # Finally, derive a generic inverse relationship
        # for relationships that do not have one.
        df['inverse_label'] = np.where(
            df['inverse_label'].isnull(),
            'inverse_' + df['relation_label'],
            df['inverse_label']
        )


        df['relation_label'] = self._format_relationship_for_neo4j(df['relation_label'])
        df['inverse_label'] = self._format_relationship_for_neo4j(df['inverse_label'])

        utime.stop()

        return df['relation_label']

    def _format_relationship_for_neo4j(self,x: pd.Series) -> pd.Series:

        """
        # Converts strings that correspond to a predicate string to a
        format suitable for use in a neo4j database.
        The objective is to avoid needing back-ticks in Cypher queries.

        :param x: Pandas Series object containing predicates (edges)
        :return: Pandas Series object of standardized predicates.
        """

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
        ret = ret.str.replace(' ', '_', regex=False)

        ret = ret.str.lower()
        ret = np.where(ret.astype(str).str[0].str.isnumeric(),'rel_' + ret, ret)

        return ret

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

        self.ulog.print_and_logger_info('Obtaining relationship reference information from Relations Ontology...')
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
            columns={'id_x': 'IRI', 'lbl_x': 'relation_label_RO', 'id_y': 'inverse_IRI', 'lbl_y': 'inverse_label_RO'})
        # The inverses are undefined.
        dfnoinv['inverse_IRI'] = np.nan
        dfnoinv['inverse_label_RO'] = np.nan

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
        dfnoinv['inverse_label_RO'] = np.where(dfnoinv['lbl'].isnull(), dfnoinv['inverse_label_RO'], dfnoinv['lbl'])
        dfnoinv = dfnoinv[['IRI', 'relation_label_RO', 'inverse_IRI', 'inverse_label_RO']]

        # ---------------------------------
        # Filter the base triples frame to just those relationship properties that have inverses.
        # This step eliminates relationship properties related by relationships such as "subPropertyOf".
        dfrt = dfrt[dfrt['pred'] == 'inverseOf']

        # Rename column names.
        # Column names will be:
        # IRI - the IRI for the relationship property
        # relation_label_RO - the label for the relationship property
        # inverse_IRI - IRI for the inverse relationship (This will be dropped.)
        # inverse_label_RO - the label of the inverse relationship property
        dfrt = dfrt[['id_x', 'lbl_x', 'id_y', 'lbl_y']].rename(
            columns={'id_x': 'IRI', 'lbl_x': 'relation_label_RO', 'id_y': 'inverse_IRI', 'lbl_y': 'inverse_label_RO'})

        # Add triples for problematic relationship properties--i.e., without inverses or from incomplete pairs.
        dfrt = pd.concat([dfrt, dfnoinv], ignore_index=True).drop_duplicates(subset=['IRI'])

        # Convert stings for labels for relationships to expected delimiting.
        dfrt['relation_label_RO'] = \
            dfrt['relation_label_RO'].str.replace(' ', '_').str.split('/').str[-1]
        dfrt['inverse_label_RO'] = dfrt['inverse_label_RO'].str.replace(' ', '_').str.split('/').str[-1]

        dfrt = dfrt.drop(columns='inverse_IRI')

        # Cast IRI to lower case.
        dfrt['IRI'] = dfrt['IRI'].str.lower()

        return dfrt

    def _get_relationships_from_bspo(self)-> pd.DataFrame:

        """
        Obtains relationship labels from the Biological Spatial Ontology.

        :return: DataFrame
        """

        self.ulog.print_and_logger_info('Obtaining relationship reference information from Biological Spatial Ontology...')
        # Fetch the RO JSON.
        dfbspo = pd.read_json("https://raw.githubusercontent.com/obophenotype/biological-spatial-ontology/refs/heads/master/bspo-base.json")

        # Information on relationship properties is in the nodes array.
        # Cast strings (IRI) to lowercase.
        dfrelnodes = pd.DataFrame(dfbspo.graphs[0]['nodes']).apply(
            lambda x: x.str.lower() if x.dtype == 'object' else x)
        return dfrelnodes
