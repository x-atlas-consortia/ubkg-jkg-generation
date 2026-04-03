#!/usr/bin/env python
# coding: utf-8
# 2026
# Unified Biomedical Knowledge Graph - JSON Knowledge Graph (UBKG-JKG)
# Script to ingest GenCode data

import os
import sys

import argparse
from tqdm import tqdm
import pandas as pd
import numpy as np

# Import UBKG utilities which is in a directory that is at the same level as the script directory.
# Go "up and over" for an absolute path.
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/ubkg_utilities')
sys.path.append(fpath)

# argparser
from ubkg_args import RawTextArgumentDefaultsHelpFormatter
# Centralized logging module
from find_repo_root import find_repo_root
from ubkg_logging import ubkgLogging

# config file
from ubkg_config import ubkgConfigParser

# Extraction module
from ubkg_extract import ubkgExtract
# -----------------------------

def download_source_files(cfg: ubkgConfigParser, uext:ubkgExtract, sab_source_dir: str, sab_jkg_dir: str) -> list[str]:
    """
    Obtains source files from the GENCODE FTP site.
    :param cfg: an instance of the ubkgConfigParser class, which works with the application configuration file.
    :param uext: an instance of the UbkgExtract class
    :param sab_source_dir: location of downloaded GenCode GZIP files
    :param sab_jkg_dir:  location of extracted GenCode GTF files
    :return:
    """

    # Create output folders for source files. Use the existing sab_source and sab_jkg folder structure.
    os.system(f'mkdir -p {sab_source_dir}')
    os.system(f'mkdir -p {sab_jkg_dir}')

    # Download files specified in a list of URLs.
    list_gtf = []

    for key in cfg.config['URL']:
        url = cfg.get_value(section='URL', key=key)
        # The URL contains the filename.
        zipfilename = url.split('/')[-1]
        list_gtf.append(uext.get_gzipped_file(zip_url=url, zip_path=sab_source_dir, extract_path=sab_jkg_dir, zipfilename=zipfilename))

    return list_gtf


def load_gtf_into_dataframe(ulog:ubkgLogging, uext:ubkgExtract, file_pattern: str, path: str, skip_lines: int=0, rows_to_read: int=0) -> pd.DataFrame:

    """
    Loads a GTF file into a Pandas DataFrame, showing a progress bar.
    :param ulog: ubkgLogging instance
    :param uext: UbkgExtract instance
    :param file_pattern: portion of a name of a GTF file--e.g., "annotation"
    :param path: path to folder containing GTF files.
    :param skip_lines: number of lines to skip
    :param rows_to_read: optional number of rows to read. In this case, the default means to read all rows.
    :return:
    """

    list_gtf = os.listdir(path)

    for filename in list_gtf:
        if file_pattern in filename:
            gtffile = os.path.join(path, filename)
            ulog.print_and_logger_info(f'Reading {gtffile}')
            return uext.read_csv_with_progress_bar(path=gtffile, rows_to_read=rows_to_read, comment='#', sep='\t')

    # ERROR condition
    ulog.print_and_logger_info(f'Error: missing file with name that includes \'{file_pattern}\'.')
    exit(1)


def build_key_value_column(df_gtfl1: pd.DataFrame, search_key: str):

    """
    Builds a consolidated value column from a key-value column in GTF format and adds it to the
    input DataFrame.
    Refer to https://www.gencodegenes.org/pages/data_format.html for the key-value column.

    :param df_gtfl1: DataFrame of data downloaded from FTP, after the key-value column has been split by the
                    Level 1 delimiter (colon)
    :param search_key: name of the key to extract from the key-value column.
    :return:
    """

    # -----------------------------
    # Level 2 split
    # Split each column from the Level 1 split into key and value columns.
    listval = []

    for col in df_gtfl1:
        # The columns from the first-level split have names that are numbers starting with 0.
        if not isinstance(col, int):
            continue

        # Split each key/value pair column into separate key and value columns, using the space delimiter.
        # The strip function removes leading spaces that can be mistaken for delimiters, such as occurs in the
        # first key-value column.
        # Incorporate a progress bar.
        # tqdm.pandas(desc='splitting')
        df_split_level_2 = df_gtfl1[col].str.split(' ', expand=True).apply(lambda x: x.str.strip())

        # The split gives the key and value columns numeric names. Rename for clarity.
        df_split_level_2.columns = ['key', 'value']

        # Obtain values that correspond to the search key
        # In general, there are multiple values for a key on a row, so there will be multiple rows in the
        # dataframe with the same index value.
        df_split_level_2 = df_split_level_2[df_split_level_2['key'] == search_key]

        # Add any matching values to the set, organized by index.
        if df_split_level_2.shape[0] > 0:
            listval.append(df_split_level_2)

    if len(listval) > 0:
        # Build the entire list of values for the key.
        df_values = pd.concat(listval)

        # Concatenate multiple values that appear for the key in a row.
        # This collapses the result down to the correct number of rows.
        df_values = df_values.reset_index(names='rows')
        df_values = df_values.groupby('rows').agg({'value': lambda x: ','.join(x)})
        s_values = df_values['value']
    else:
        # Return an empty series.
        s_values = pd.Series(index=df_gtfl1.index.copy(), dtype='str')

    # Add the consolidated list of values for the search key to the input DataFrame.
    df_gtfl1[search_key] = s_values

    return


def split_column9_level1(df_gtf: pd.DataFrame) -> pd.DataFrame:
    """
    Perform the first-level split of the key-value column (9th) of a GTF file, adding
    separated key-value pairs as columns.

    Refer to in-line documentation in the function build_annotation_dataframe for details on the
    processing of the key-value column.

    """
    #
    key_value_column = df_gtf['column_9']

    # Split the key/value pairs using the colon delimiter.
    # Incorporate a progress bar.
    tqdm.pandas(desc='Splitting')
    df_split_level_1 = key_value_column.str.split(';', expand=True).progress_apply(lambda x: x.str.strip())

    # Normalize empty column empty values to NaN.
    # 2026 - Opt for future behavior from pandas to remove deprecation warning.
    pd.set_option('future.no_silent_downcasting', True)
    df_split_level_1 = df_split_level_1.replace({'None': np.nan}).replace({None: np.nan}).replace({'': np.nan})

   # Remove completely empty columns.
    df_split_level_1 = df_split_level_1.dropna(axis=1, how='all')

    # Add columns of separated key-value pairs to the input.

    for col in df_split_level_1:
        df_gtf[col] = df_split_level_1[col]
    return df_gtf


def filter_annotations(cfg: ubkgConfigParser, df: pd.DataFrame) -> pd.DataFrame:

    """
    Filters the annotation DataFrame by the types of annotations indicated in the application configuration file.

    :param cfg: an instance of the ubkgConfigParser class, which works with the application configuration file.
    :param df: a DataFrame of annotation information
    :return: filtered DataFrame
    """

    # Get desired feature types from the configuration file.
    feature_types = cfg.get_value(section='Filters', key='feature_types').split(',')
    if feature_types == ['all']:
        return df
    else:
        # Filter rows.
        return df[df['feature_type'].isin(feature_types)]


def filter_columns(cfg: ubkgConfigParser, df: pd.DataFrame) -> pd.DataFrame:

    """
    Reduces the set of columns in the annotation DataFrame by values indicated in the application configuration file.

    :param cfg: an instance of the ubkgConfigParser class, which works with the application configuration file.
    :param df: a DataFrame of annotation information.
    :return: filtered DataFrame
    """

    cols = cfg.get_value(section='Filters', key='columns').split(',')
    if cols == ['all']:
        return df
    else:
        # Filter columns.
        df = df[cols]
        return df

def build_annotation_dataframe(cfg: ubkgConfigParser, ulog: ubkgLogging, uext: ubkgExtract, path: str) -> pd.DataFrame:

    """
    Builds a DataFrame that translates the GenCode annotation GTF file.
    The specification of GTF files is at https://www.gencodegenes.org/pages/data_format.html

    :param ulog: ubkgLogging instance
    :param cfg: an instance of the ubkgConfigParser class, which works with the application configuration file.
    :param uext: UbkgExtract instance
    :param path: path to folder containing GTF files.
    :return: DataFrame
    """

    # Load the "raw" version of the GTF file into a DataFrame.
    # Because the GenCode version is part of the file name (e.g., gencode.v41.annotation.gtf),
    # search on a file pattern.
    # The first five rows of the annotation file are comments.

    df_gtf = load_gtf_into_dataframe(ulog=ulog, uext=uext, file_pattern="annotation", path=path, skip_lines=5)

    # The GTF file does not have column headers. Add these with values from the specification.
    df_gtf.columns = cfg.get_value(section='GTF_columns', key='columns').split(',')
    # Filter annotation rows by types listed in configuration file.
    # This will likely reduce the size of the resulting DataFrame considerably.
    df_gtf = filter_annotations(cfg=cfg, df=df_gtf)

    # Add columns corresponding to the key/value pairs in the 9th column.
    # GTF key names are from the specification.
    list_keys = cfg.get_value(section='GTF_column9_keys', key='keys').split(',')

    # --------------------------
    # The key-value column uses two levels of delimiting:
    # Level 1 - delimiter between key/value pairs = ;
    # Level 2 - delimiter between key and value = ' '

    # Excerpt of a key-value column (from the general annotation GTF):
    # "gene_id "ENSG00000223972.5"; transcript_id "ENST00000456328.2"; gene_type "transcribed_unprocessed_pseudogene"; gene_name "DDX11L1";"

    # Key/value pairs do not have static locations--i.e., a key/value pair may be in column X in one row and column Y
    # in another.
    # Furthermore, some keys have multiple values in the same row. For example, row 11 of the annotation shows tag "basic" in column 11
    # and tag "Ensembl_canonical" in column 12.
    #

    # This means that the key-value columns after the Level 1 split will resemble the following
    # (x, y, z, a are keys):
    #     columns
    # row       1         2         3       4
    # 0         x 20
    # 1         x 30
    # 2                    x 40
    # 3         a 99
    # 4         y 25       z 30     x 50    x 60 <--note multiple values for the x key

    # The desired result if search_key = x is a series of values corresponding to key=x,
    # sorted in the original row order, with multiple values collected into lists--e.g.,
    #  x
    # 0 20
    # 1 30
    # 2 40
    # 3
    # 4 50,60

    # Level 1 split
    ulog.print_and_logger_info('-- Splitting the key-value column (9th) of the annotation file into individual key-value columns...')

    df_gtf['column_9'] = df_gtf['column_9'].str.replace('"', '')
    df_gtf = split_column9_level1(df_gtf)

    # Level 2 split and collect
    ulog.print_and_logger_info('-- Collecting values from key-value columns...')
    for k in tqdm(list_keys, desc='Collecting'):
        build_key_value_column(df_gtf, k)

    # Remove intermediate Level 1 columns.
    droplabels = []
    for col in df_gtf:
        if isinstance(col, int):
            droplabels.append(col)
    df_gtf = df_gtf.drop(columns=droplabels)
    return df_gtf


def build_metadata_dataframe(ulog: ubkgLogging, uext:ubkgExtract, file_pattern: str, path: str, column_headers: list[str]) -> pd.DataFrame:

    """
    Builds a DataFrame that translates one of the GenCode metadata files.
    The specification of metadata files is at https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_41/_README.TXT

    :param ulog: ubkgLogging instance
    :param uext: UbkgExtract instance
    :param file_pattern: relevant portion of a filename.
    [The GenCode version is part of the file name (e.g., gencode.v41.annotation.gtf), so search on the pattern.]
    :param path: path to folder containing GTF files.
    :param column_headers: column headers for the GTF file.
    :return:
    """
    # Load the "raw" version of the GTF file into a DataFrame.

    # search on a file pattern.
    df_gtf = load_gtf_into_dataframe(ulog=ulog, uext=uext, file_pattern=file_pattern, path=path)

    # The GTF file does not have column headers. Add these with values from the specification.
    df_gtf.columns = column_headers

    return df_gtf


def build_translated_annotation_dataframe(cfg: ubkgConfigParser, ulog: ubkgLogging, uext: ubkgExtract, path: str, outfile: str) -> pd.DataFrame:

    """
    Builds a DataFrame that:
    1. Translates the annotation GTF file.
    2. Combines translated GTF annotation data with metadata, joining by transcript_id.

    :param cfg: UbkgConfigParser class, which works with the application configuration file.
    :param ulog: ubkgLogging instance
    :param uext: UbkgExtract instance
    :param path: full path to the source GTF file
    :param outfile: output file name
    :return:
    """

    # Read GTF files into DataFrames.

    ulog.print_and_logger_info('** BUILDING TRANSLATED GTF ANNOTATION FILE **')
    # Load and translate annotation file.
    df_annotation = build_annotation_dataframe(cfg=cfg, ulog=ulog, uext=uext, path=path)

    # Metadata
    # Entrez file
    df_entrez = build_metadata_dataframe(ulog=ulog, uext=uext, file_pattern='EntrezGene', path=path,
                                        column_headers=['transcript_id', 'Entrez_Gene_id'])
    # RefSeq file
    df_refseq = build_metadata_dataframe(ulog=ulog, uext=uext, file_pattern='RefSeq', path=path,
                                        column_headers=['transcript_id', 'RefSeq_RNA_id', 'RefSeq_protein_id'])
    # SwissProt file
    df_swissprot = build_metadata_dataframe(ulog=ulog, uext=uext, file_pattern='SwissProt', path=path,
                                           column_headers=['transcript_id', 'UNIPROTKB_SwissProt_AN',
                                                           'UNIPROTKB_SwissProt_AN2'])
    # TrEMBL file
    df_trembl = build_metadata_dataframe(ulog=ulog, uext=uext, file_pattern='TrEMBL', path=path,
                                        column_headers=['transcript_id', 'UNIPROTKB_TrEMBL_AN', 'UNIPROTKB_TrEMBL_AN2'])

    # Join Metadata files to Annotation file.
    ulog.print_and_logger_info('-- Merging annotation and metadata.')
    df_annotation = df_annotation.merge(df_entrez, how='left', on='transcript_id')
    df_annotation = df_annotation.merge(df_refseq, how='left', on='transcript_id')
    df_annotation = df_annotation.merge(df_swissprot, how='left', on='transcript_id')
    df_annotation = df_annotation.merge(df_trembl, how='left', on='transcript_id')

    # Filter output by columns as indicated in the configuration file.
    df_annotation = filter_columns(cfg=cfg, df=df_annotation)

    # Write translated annotation file.
    outfile_ann = os.path.join(path, outfile)
    ulog.print_and_logger_info(f'-- Writing to {outfile_ann}')
    uext.to_csv_with_progress_bar(df=df_annotation, path=outfile_ann)
    return df_annotation


def getargs() -> argparse.Namespace:

    # Parse arguments.
    parser = argparse.ArgumentParser(
    description='Convert GENCODE annotation files to OWLNETs',
    formatter_class=RawTextArgumentDefaultsHelpFormatter)
    # positional arguments
    parser.add_argument("-f", "--fetchnew", action="store_true", help="fetch new set of annotation files ")
    args = parser.parse_args()

    return args


def stripped_ensembl_id(ensembl: str) -> str:

    # Strips the version number from an ENSEMBL ID.
    return ensembl.split('.')[0]


def get_ensembl_version(ensembl: str) -> str:

    # Obtains the version number from an ENSEMBL ID.
    return ensembl.split('.')[1]

def write_edges_file(ulog: ubkgLogging, uext: ubkgExtract, df: pd.DataFrame, path: str, vs_path: str):

    """
    Translates the content of a GTF annotation file to OWLNETS format.
    :param ulog: ubkgLogging object
    :param df: DataFrame of annotated GTF information.
    :param path: export path of OWLNETS files
    :param vs_path: path to the directory containing OWLNETS files related to the ingestion of the GENCODE_VS ontology
    :return:
    """

    # Assertions on transcripts:
    # - transcribed from genes, using Ensembl IDs for genes
    # - has proteins as gene products, using the UNIPROTKB IDs

    # The object node IDs for assertions for which subjects are features are obtained from the
    # OWLNETS_node_metadata file for the GENCODE_VS set of assertions.
    # Feature assertions:
    # 1. Feature is located in a chromosome
    # 2. Feature has feature type
    # 3. Feature has biotype, based on whether the feature is a gene (gene_type) or not (transcript_type)
    # 4. Feature has directional form (strand direction)
    # 5. Feature isa for all PGO codes in the ont field

    # Pandas sets the type of a column for which the first row is null to float.

    # Read the node information from GENCODE_VS.
    df_gencode_vs = get_gencode_vs(ulog=ulog, uext=uext, path=vs_path)

    edgelist_path: str = os.path.join(path, 'OWLNETS_edgelist.txt')
    ulog.print_and_logger_info('Building: ' + os.path.abspath(edgelist_path))

    with open(edgelist_path, 'w') as out:
        # header
        out.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')

        # ASSERTIONS FOR TRANSCRIPTS
        ulog.print_and_logger_info('Writing \'transcribed from\' and \'has gene product\' edges for transcripts')
        # Identify unique transcript IDs.
        dftranscript = df[df['feature_type'] == 'transcript']
        dftranscript = dftranscript.drop_duplicates(subset=['transcript_id']).reset_index(drop=True)

        # Show TQDM progress bar.
        for index, row in tqdm(dftranscript.iterrows(), total=dftranscript.shape[0]):
            # July 2023 - strip version from Ensembl IDs.
            subj = 'ENSEMBL:' + stripped_ensembl_id(row['transcript_id'])
            obj = 'ENSEMBL:' + stripped_ensembl_id(row['gene_id'])

            # ASSERTION: transcribed_from
            predicate = 'http://purl.obolibrary.org/obo/RO_0002510' # transcribed from
            out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            # ASSERTIONs: has_gene_product
            # Look for proteins in both SwissProt and Trembl annotations of UniProtKB
            predicate = 'http://purl.obolibrary.org/obo/RO_0002205' # has_gene_product
            if row['UNIPROTKB_SwissProt_AN'] != '':
                obj = f'UNINPROTKB:{row["UNIPROTKB_SwissProt_AN"]}'
                out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            if row['UNIPROTKB_TrEMBL_AN'] != '':
                obj = f'UNIPROTKB:{row["UNIPROTKB_TrEMBL_AN"]}'
                out.write(subj + '\t' + predicate + '\t' + obj + '\n')

        # ASSERTIONS for features (genes, transcripts, etc.)
        ulog.print_and_logger_info('Writing edges for all features (gene, transcript, etc.)--chromosome, biotype, direction, pseudogene, RefSeq')
        for index, row in tqdm(df.iterrows(), total=df.shape[0]):

            # feature ID
            # July 2023 - Strip Ensembl IDs.
            if row['transcript_id'] != '':
                subj = f'ENSEMBL:{stripped_ensembl_id(row["transcript_id"])}'
            else:
                subj = f'ENSEMBL:{stripped_ensembl_id(row["gene_id"])}'

            obj = ''

            # Assertion: (feature) located in (chromosome)
            predicate = 'http://purl.obolibrary.org/obo/RO_0001025' # located in
            # Obtain from GENCODE_VS the node_id for the node that corresponds
            # to the value from the chromosome_name column.
            if df_gencode_vs.loc[df_gencode_vs['node_label']==row['chromosome_name'],'node_id'].shape[0] > 0:
                obj = str(df_gencode_vs.loc[df_gencode_vs['node_label'] == row['chromosome_name'], 'node_id'].iat[0])
                if obj != '':
                    out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            obj = ''
            # Assertion: (feature) has feature type (feature type)
            # There is currently no appropriate relation property in RO.
            predicate = 'is_feature_type'
            # Obtain from GENCODE_VS the node_id for the feature type
            if df_gencode_vs.loc[df_gencode_vs['node_label'] == row['feature_type'], 'node_id'].shape[0] > 0:
                obj = str(df_gencode_vs.loc[df_gencode_vs['node_label'] == row['feature_type'], 'node_id'].iat[0])
                if obj !='':
                    out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            obj = ''
            # Assertion: (feature) is gene biotype
            # There is currently no appropriate relation property in RO.
            predicate = 'is_gene_biotype'
            if row['gene_type'] != '':
                if df_gencode_vs.loc[df_gencode_vs['node_label'] == row['gene_type'], 'node_id'].shape[0] > 0:
                    obj = str(df_gencode_vs.loc[df_gencode_vs['node_label'] == row['gene_type'], 'node_id'].iat[0])
                    if obj != '':
                        out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            obj = ''
            # Assertion: (feature) is transcript biotype
            # There is currently no appropriate relation property in RO.
            predicate = 'is_transcript_biotype'
            if row['transcript_type'] != '':
                if df_gencode_vs.loc[df_gencode_vs['node_label'] == row['transcript_type'], 'node_id'].shape[0] > 0:
                    obj = str(df_gencode_vs.loc[df_gencode_vs['node_label'] == row['transcript_type'], 'node_id'].iat[0])
                if obj != '':
                    out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            obj = ''
            # Assertion: (feature) has directional form of (strand)
            direction = ''
            predicate ='http://purl.obolibrary.org/obo/RO_0004048' # has directional form of
            if row['genomic_strand'] == '+':
                direction = 'positive'
            if row['genomic_strand'] == '-':
                direction = 'negative'

            if direction != '':
                if df_gencode_vs.loc[df_gencode_vs['node_label'] == direction, 'node_id'].shape[0] > 0:
                    obj = str(df_gencode_vs.loc[df_gencode_vs['node_label'] == direction, 'node_id'].iat[0])
            if obj != '':
                out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            obj = ''
            # Assertion: isa (type of Pseudogene)
            # Assume that the ont field can be a list of PGO IDs.
            # Assume that PGO nodes were ingested prior to the GENCODE ingestion.
            predicate = 'subClassOf'
            if str(row['ont']).strip() != '':
                list_pgo = str(row['ont']).split(',')
                for pgo in list_pgo:
                    # JULY 2023 SAB:code format
                    # Replace colon with underscore for codeReplacements function.
                    # obj = f'PGO_{pgo.split(":")[-1]}'
                    obj = pgo
                    out.write(subj + '\t' + predicate + '\t' + obj + '\n')

            obj = ''
            # Assertion: has refSeq ID
            # The RefSeq nodes will be created as part of the GENCODE ingestion.
            # JULY 2023 - SAB:code format
            predicate = 'has_refSeq_ID'
            if row['RefSeq_RNA_id'] != '':
                obj = f'REFSEQ:{row["RefSeq_RNA_id"]}'
                out.write(subj + '\t' + predicate + '\t' + obj + '\n')
            if row['RefSeq_protein_id'] != '':
                obj = f'REFSEQ:{row["RefSeq_protein_id"]}'
                out.write(subj + '\t' + predicate + '\t' + obj + '\n')

    return

def write_nodes_file(ulog: ubkgLogging, df: pd.DataFrame, path: str):

    """
    Writes a nodes file in OWLNETS format.
    :param ulog: ubkgLogging object
    :param df: DataFrame of source information
    :param path: output directory
    :return:
    """

    # The primary annotation nodes information is the set of cross-references, including:
    # - HGNC and Entrez IDs for Ensembl gene IDs
    # - RefSeq RNA IDs for transcripts

    # The Entrez IDs for genes are associated with the gene's transcripts. The Entrez ID is the same for all
    # of a gene's transcripts.

    node_metadata_path: str = os.path.join(path, 'OWLNETS_node_metadata.txt')
    ulog.print_and_logger_info('Building: ' + os.path.abspath(node_metadata_path))

    # Get subsets of annotations by feature type.
    # gene
    dfgene = df[df['feature_type'] == 'gene']
    dfgene = dfgene.drop_duplicates(subset=['gene_id']).reset_index(drop=True)
    dfgene = dfgene.replace(np.nan, '')
    # transcript
    dftranscript = df[df['feature_type'] == 'transcript']
    dftranscript = dftranscript.drop_duplicates(subset=['transcript_id']).reset_index(drop=True)
    dftranscript = dftranscript.replace(np.nan, '')

    with open(node_metadata_path, 'w') as out:
        out.write(
            'node_id' + '\t' + 'node_namespace' + '\t' + 'node_label' + '\t' + 'node_definition' + '\t' +
            'node_synonyms' + '\t' + 'node_dbxrefs' + '\t' + 'value' + '\t' + 'lowerbound' + '\t' +
            'upperbound' + '\t' + 'unit' + '\n')

        # GENE NODES
        ulog.print_and_logger_info('Writing gene nodes')
        # Find unique gene nodes.

        # Show TQDM progress bar.
        for index, row in tqdm(dfgene.iterrows(), total=dfgene.shape[0]):
            # July 2023 - Strip version from Ensembl ID
            node_id = f'ENSEMBL:{stripped_ensembl_id(row["gene_id"])}'
            node_namespace = 'GENCODE'
            node_label = row['gene_name'].strip()

            # July 2023 - store the full Ensembl ID, including version, as value.
            value = get_ensembl_version(row["gene_id"])
            node_definition = ''
            node_synonyms = ''

            dbxreflist = []
            # JULY 2023 - Format changed from HGNC HGNC:code to HGNC:code
            if row['hgnc_id'] != '':
                # dbxreflist.append('HGNC ' + row['hgnc_id'])
                dbxreflist.append(row['hgnc_id'])
            # if row['mgi_id'] != '':
                # dbxreflist.append('MGI:' + row['mgi_id'])

            node_dbxrefs = ''
            if len(dbxreflist) > 0:
                node_dbxrefs = '|'.join(dbxreflist)

            # value = ''
            lowerbound = str(int(row['genomic_start_location']))
            upperbound = str(int(row['genomic_end_location']))
            unit = ''

            out.write(
                node_id + '\t' + node_namespace + '\t' + node_label + '\t' + node_definition + '\t'
                + node_synonyms + '\t' + node_dbxrefs + '\t' + value + '\t' + lowerbound + '\t' + upperbound + '\t' + unit + '\n')

        # TRANSCRIPT NODES
        # Group by transcript_id and RefSeq_RNA_id.
        ulog.print_and_logger_info('Writing transcript nodes')

        for index, row in tqdm(dftranscript.iterrows(), total=dftranscript.shape[0]):
            # July 2023 - Strip version from Ensembl ID.
            node_id = f'ENSEMBL:{stripped_ensembl_id(row["transcript_id"])}'
            node_namespace = 'GENCODE'
            node_label = row['transcript_name']
            node_definition = ''
            node_synonyms = ''
            # July 2023 - provide full Ensembl ID as value.
            value = get_ensembl_version(row["transcript_id"])
            lowerbound = str(int(row['genomic_start_location']))
            upperbound = str(int(row['genomic_end_location']))
            unit = ''
            node_dbxrefs = ''

            out.write(node_id + '\t' + node_namespace + '\t' + node_label + '\t' + node_definition + '\t'
                      + node_synonyms + '\t' + node_dbxrefs + '\t' + value + '\t' + lowerbound + '\t' + upperbound + '\t' + unit + '\n')

        # ENTREZ GENE NODES
        # These are available in the annotation file, but are not involved in edges.
        # Map them to HGNC IDs.
        ulog.print_and_logger_info('Writing Entrez nodes')

        df_entrez = dftranscript[dftranscript['Entrez_Gene_id'] != '']
        df_entrez = df_entrez.drop_duplicates(subset=['Entrez_Gene_id']).reset_index(drop=True)
        for index, row in tqdm(df_entrez.iterrows(), total=df_entrez.shape[0]):
            node_id = f'ENTREZ:{int(row["Entrez_Gene_id"])}'
            node_namespace = 'GENCODE'
            node_label = row['gene_name']
            node_definition = ''
            node_synonyms = ''
            value = ''
            lowerbound = str(int(row['genomic_start_location']))
            upperbound = str(int(row['genomic_end_location']))
            unit = ''
            # July 2023 - Format changed from HGNC HGNC:code to HGNC:code
            # node_dbxrefs = 'HGNC ' + row['hgnc_id']
            node_dbxrefs = row['hgnc_id']
            out.write(node_id + '\t' + node_namespace + '\t' + node_label + '\t' + node_definition + '\t'
                      + node_synonyms + '\t' + node_dbxrefs + '\t' + value + '\t' + lowerbound + '\t' + upperbound + '\t' + unit + '\n')

        # REFSEQ RNA NODES
        # These are available in the annotation file.
        ulog.print_and_logger_info('Writing RefSeq RNA nodes')
        dfRefSeq = df[df['RefSeq_RNA_id'] != '']
        dfRefSeq = dfRefSeq.drop_duplicates(subset=['RefSeq_RNA_id']).reset_index(drop=True)
        for index, row in tqdm(dfRefSeq.iterrows(), total=dfRefSeq.shape[0]):
            # JULY 2023 - SAB:code
            node_id = f'REFSEQ:{row["RefSeq_RNA_id"]}'
            node_namespace = 'GENCODE'
            node_label = row['RefSeq_RNA_id']
            node_definition = ''
            node_synonyms = ''
            value = ''
            lowerbound = ''
            upperbound = ''
            unit = ''
            node_dbxrefs = ''
            out.write(node_id + '\t' + node_namespace + '\t' + node_label + '\t' + node_definition + '\t'
                      + node_synonyms + '\t' + node_dbxrefs + '\t' + value + '\t' + lowerbound + '\t' + upperbound + '\t' + unit + '\n')

        # REFSEQ RNA NODES
        # These are available in the annotation file.
        ulog.print_and_logger_info('Writing RefSeq protein nodes')
        dfRefSeq = df[df['RefSeq_RNA_id'] != '']
        dfRefSeq = dfRefSeq.drop_duplicates(subset=['RefSeq_protein_id']).reset_index(drop=True)
        for index, row in tqdm(dfRefSeq.iterrows(), total=dfRefSeq.shape[0]):
            # July 2023 - SAB:code
            node_id = f'REFSEQ:{row["RefSeq_protein_id"]}'
            node_namespace = 'GENCODE'
            node_label = row['RefSeq_protein_id']
            node_definition = ''
            node_synonyms = ''
            value = ''
            lowerbound = ''
            upperbound = ''
            unit = ''
            node_dbxrefs = ''
            out.write(node_id + '\t' + node_namespace + '\t' + node_label + '\t' + node_definition + '\t'
                          + node_synonyms + '\t' + node_dbxrefs + '\t' + value + '\t' + lowerbound + '\t' + upperbound + '\t' + unit + '\n')

    return

def write_relations_file(ulog:ubkgLogging, path: str):

    """
    Writes a relations file in OWLNETS format.
    :param path: output directory
    :return:
    """

    # RELATION METADATA
    # Create a row for each type of relationship.

    relation_path: str = os.path.join(path, 'OWLNETS_relations.txt')
    ulog.print_and_logger_info('Building: ' + os.path.abspath(relation_path))

    with open(relation_path, 'w') as out:
        # header
        out.write(
            'relation_id' + '\t' + 'relation_namespace' + '\t' + 'relation_label' + '\t' + 'relation_definition' + '\n')
        relation1_id = 'http://purl.obolibrary.org/obo/RO_0002510' # transcribed from
        relation1_label = 'transcribed from'
        relation2_id = 'http://purl.obolibrary.org/obo/RO_0002205' # has_gene_product
        relation2_label = 'has gene product'
        relation3_id = 'http://purl.obolibrary.org/obo/RO_0001025'  # located in
        relation3_label = 'located in'
        relation4_id = 'is_feature_type'
        relation4_label='is_feature_type'
        relation5_id = 'is_gene_biotype'
        relation5_label ='is_gene_biotype'
        relation6_id = 'is_transcript_biotype'
        relation6_label = 'is_transcript_biotype'
        relation7_id = 'http://purl.obolibrary.org/obo/RO_0004048' # has directional form of
        relation7_label = 'has_directional_form_of'
        relation8_id = 'subclassOf'
        relation8_label = 'subClassOf'
        relation9_id = 'has_refSeq_ID'
        relation9_label = 'has_refSeq_ID'

        out.write(relation1_id + '\t' + 'GENCODE' + '\t' + relation1_label + '\t' + '' + '\n')
        out.write(relation2_id + '\t' + 'GENCODE' + '\t' + relation2_label + '\t' + '' + '\n')
        out.write(relation3_id + '\t' + 'GENCODE' + '\t' + relation3_label + '\t' + '' + '\n')
        out.write(relation4_id + '\t' + 'GENCODE' + '\t' + relation4_label + '\t' + '' + '\n')
        out.write(relation5_id + '\t' + 'GENCODE' + '\t' + relation5_label + '\t' + '' + '\n')
        out.write(relation6_id + '\t' + 'GENCODE' + '\t' + relation6_label + '\t' + '' + '\n')
        out.write(relation7_id + '\t' + 'GENCODE' + '\t' + relation7_label + '\t' + '' + '\n')
        out.write(relation8_id + '\t' + 'GENCODE' + '\t' + relation8_label + '\t' + '' + '\n')
        out.write(relation9_id + '\t' + 'GENCODE' + '\t' + relation9_label + '\t' + '' + '\n')
    return

def get_gencode_vs(ulog: ubkgLogging, uext: ubkgExtract, path: str) -> pd.DataFrame:
    """

    :param ulog: ubkgLogging
    :param uext: UbkgExtract
    :param path: path to the GENCODE_VS directory in the repository
    :return:
    """

    # The GENCODE annotation file has columns with values that have been encoded as nodes in the GENCODE_VS
    # set of assertions.
    # Load the nodes file related to the prior ingestion.

    nodefile = os.path.join(path, 'OWLNETS_node_metadata.txt')

    try:
        return uext.read_csv_with_progress_bar(nodefile, sep='\t')
    except FileNotFoundError:
        ulog.print_and_logger_info('GENCODE depends on the prior ingestion of information '
                                   'from the GENCODE_VS SAB. Run .build_csv.sh for GENCODE_VS prior '
                                   'to running it for GENCODE.')
        exit(1)

def main():

    # Locate the root directory of the repository for absolute
    # file paths.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    # Set up centralized logging.
    ulog = ubkgLogging(log_dir=log_dir, log_file='ubkg.log')

    # Obtain runtime arguments.
    args = getargs()

    # Get application configuration.
    cfgpath = os.path.join(os.path.dirname(os.getcwd()), 'generation_framework/gencode/gencode.ini')
    cfg = ubkgConfigParser(path=cfgpath, log_dir=log_dir, log_file='ubkg.log')

    # Get sab_source and sab_jkg directories.
    # The config file contains absolute paths to the parent directories in the local repo.
    # Affix the SAB to the paths.
    sab_source_dir = os.path.join(os.path.dirname(os.getcwd()),
                                  cfg.get_value(section='Directories', key='sab_source_dir'), 'GENCODE')
    sab_jkg_dir = os.path.join(os.path.dirname(os.getcwd()), cfg.get_value(section='Directories', key='sab_jkg_dir'),
                               'GENCODE')

    # Obtain the path to the OWLNETS files that correspond to the application ontology information for GenCode (GENCODE_VS)
    gencode_vs_dir = os.path.join(os.path.dirname(os.getcwd()), cfg.get_value(section='Directories', key='vs_dir'))

    # Obtain output name for the translated annotation file.
    ann_file = cfg.get_value(section='AnnotationFile', key='filename')

    # Instantiate UbkgExtract class
    uext = ubkgExtract(log_dir=log_dir, log_file='ubkg.log')
    if args.fetchnew:

        # Download and decompress GZIP files of GENCODE content from FTP site.
        lst_gtf = download_source_files(cfg=cfg, uext=uext, sab_source_dir=sab_source_dir, sab_jkg_dir=sab_jkg_dir)
        # Build the DataFrame that combines translated GTF annotation data with metadata.
        df_annotation = build_translated_annotation_dataframe(cfg=cfg, ulog=ulog, uext=uext, path=sab_jkg_dir, outfile=ann_file)
    else:
        # Read previously generated annotation CSV.
        path = os.path.join(sab_jkg_dir, ann_file)
        ann_rows=0
        df_annotation = uext.read_csv_with_progress_bar(path=path, rows_to_read=ann_rows)

    df_annotation = df_annotation.replace(np.nan, '')

    write_edges_file(ulog=ulog, uext=uext, df=df_annotation, path=sab_jkg_dir, vs_path=gencode_vs_dir)
    write_nodes_file(ulog=ulog, df=df_annotation, path=sab_jkg_dir)
    #write_relations_file(ulog=ulog, path=sab_jkg_dir)

if __name__ == "__main__":
    main()

