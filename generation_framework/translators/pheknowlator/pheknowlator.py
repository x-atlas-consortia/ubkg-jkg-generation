#!/usr/bin/env python
"""
2026

This script uses the PheKnowLator package to convert a file in OWL serialization to
a set of files that corresponds to a version of OWLNETS format.

Refactored for use with JKG. The original form for this script was written prior to 2023.

Script functionality is based on the Example Application provided in the PheKnowLator GitHub at
https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/OWLNETS_Example_Application.ipynb

Early (paleo-) versions of the script assumed well-behaved publications of OWL files in RDF/XML format, downloaded from
reference sites such as NCBO BioPortal or OBO Foundry.

The script has been expanded to account for a number of edge cases, including:
1. Files in serializations other than RDF/XML, such as Turtle. (OWL/XML and SKOS should probably be handled, too.)
2. Files that are Gzipped, including those that do not have a gz extension.
3. Local files (instead of downloaded ones)
4. Files with "bad lines"--e.g., inline EOFs.

Note: this script executes GNU wget from the os command line to download OWL files.

Primary differences between the output of this script (for JKG) and
canonical OWLNETS:
1. OWLNETS includes 3 types of files--edges; nodes; and relationships.
   The relations file is redundant for the purposes of UBKG-JKG.
2. OWLNETS stores dode IDs (node_id in node file; subject and object ids in edge file) as full IRIs.
   UBKG needs codes in a standard SAB:Code format.

"""
import os
import glob
import sys
import time
from datetime import timedelta

# for processing command line arguments
import argparse
import subprocess
import hashlib
from typing import Dict

# Related to the PheKnowLator package
import pkt_kg as pkt
import re
from rdflib import Graph
from rdflib.namespace import OWL, RDF, RDFS

# Progress monitoring
from tqdm import tqdm

# Working with XML
from lxml import etree

# Working with tabular data
import pandas as pd

# The following allows for an absolute import from an adjacent script directory
# in the repo--i.e., up and over instead of down.
# Find the absolute path. (This assumes that this script is being called as a subprocess.)
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/utilities')
sys.path.append(fpath)

# Centralized logging
from functions.find_repo_root import find_repo_root
from classes.ubkg_logging import ubkgLogging

# Various forms of obtaining files.
from classes.ubkg_extract import ubkgExtract

# For working with command line arguments
from classes.ubkg_args import RawTextArgumentDefaultsHelpFormatter

# For standardizing code and relationship IDs.
from classes.ubkg_standardizer import ubkgStandardizer

# For a spinning timer to wrap around block processes
from classes.ubkg_timer import UbkgTimer

# sources.json handling
from classes.ubkg_sources import ubkgSources

# Config file
from classes.ubkg_config import ubkgConfigParser

def get_args(ulog:ubkgLogging) -> argparse.Namespace:
    """
    Processes command line arguments.
    :param ulog: logging object

    :return: argparse.Namespace

    The original version of the script relied on numerous runtime arguments
    instead of configuration files.

    """

    # Process arguments.
    parser = argparse.ArgumentParser(
        description='Run PheKnowLator on OWL file (required parameter).\n'
                    'Before running check to see if there are imports in the OWL file and exit if so'
                    'unless the --with_imports switch is also given.\n'
                    '\n'
                    'In general you should not have the change any of the optional arguments',
        formatter_class=RawTextArgumentDefaultsHelpFormatter)
    parser.add_argument('owl_url', type=str,
                        help='url for the OWL file to process')
    parser.add_argument('owl_sab', type=str,
                        help='directory in --owlnets_dir and --owl_dir to save information from this run')
    parser.add_argument("-l", "--owlnets_dir", type=str, default='./owlnets_output',
                        help='directory used for the owlnets output files')
    parser.add_argument("-o", "--owl_dir", type=str, default='./owl',
                        help='directory used for the owl input files')
    parser.add_argument("-t", "--owltools_dir", type=str, default='./pkt_kg/libs',
                        help='directory where the owltools executable is downloaded to')
    # should always be true
    #parser.add_argument("-c", "--clean", action="store_true",
                        #help='clean the owlnets_output directory of previous output files before run')
    parser.add_argument("-f", "--force_owl_download", action="store_true",
                        help='force downloading of the .owl file before processing')
    # always true
    parser.add_argument("-i", "--ignore_owl_md5", action="store_true",
                        help='ignore differences between .owl MD5 and saved MD5')
    # always true
    # parser.add_argument("-w", "--with_imports", action="store_true",
                        #help='process OWL file even if imports are found, otherwise give up with an error')
    # always false
    # parser.add_argument("-D", "--delete_definitions", action="store_true",
                        #help='delete the definitions column when writing files')
    # always false
    parser.add_argument("-r", "--robot", action="store_true",
                        help='apply robot to owl_url incorporating the includes and exit')
    # always true
    parser.add_argument("-v", "--verbose", action="store_true",
                        help='increase output verbosity')

    args = parser.parse_args()

    # Document arguments.
    print_divider(ulog=ulog)
    ulog.print_and_logger_info('PHEKNOWLATOR PARAMETERS:')
    #if args.clean is True:
        #ulog.print_and_logger_info(" * Cleaning owlnets directory")
    ulog.print_and_logger_info(f" * Owl URL: {args.owl_url}")
    ulog.print_and_logger_info(f" * Owl sab: {args.owl_sab}")
    ulog.print_and_logger_info(f" * Owlnets directory: {args.owlnets_dir} (exists: {os.path.isdir(args.owlnets_dir)})")
    ulog.print_and_logger_info(f" * Owltools directory: {args.owltools_dir} (exists: {os.path.isdir(args.owltools_dir)})")
    ulog.print_and_logger_info(f" * Owl directory: {args.owl_dir} (exists: {os.path.isdir(args.owl_dir)})")

    if args.force_owl_download is True:
        ulog.print_and_logger_info(f" * PheKnowLator will force .owl file downloads")
    #if args.with_imports is True:
        #ulog.print_and_logger_info(f" * PheKnowLator will run even if imports are found in .owl file")
    #if args.delete_definitions is True:
        #ulog.print_and_logger_info(f" * Delete definitions column in the output .txt files")

    return args

def file_from_uri(uri_str: str) -> str:
    """
    Obtains a filename from a URI.
    :param uri_str: the URI
    :return: the filename
    """
    # JAS May 2023 updated to account for case of path_str being a simple file name with no '/' characters.
    # This handles the case in which the OWL file is not downloaded from a remote site,
    # but made available locally.
    if uri_str.find('/')!=-1:
        return uri_str.rsplit('/', 1)[1]
    else:
        return uri_str

def file_from_path(path_str: str) -> str:

    """
    Obtains a filename from a path.
    :param path_str: path
    :return: filename
    """
    i = path_str.rfind(os.sep)
    if i > 0 & i < len(path_str)-1:
        return path_str[i+1:]
    return None

def download_owltools(ulog:ubkgLogging, loc: str) -> None:

    """
    Downloads OWLTools, a JAR package used by the PheKnowLator.
    :param ulog: logging object
    :param loc: path to the tools directory
    :return:
    """

    owl_tools_url = 'https://github.com/callahantiff/PheKnowLator/raw/master/pkt_kg/libs/owltools'

    cmd = os.system(f"ls {loc}{os.sep}owltools > /dev/null 2>&1")
    if os.WEXITSTATUS(cmd) != 0:
        print_divider(ulog=ulog)
        ulog.print_and_logger_info.info(f'Downloading the owltools JAR at {loc} for the PheKnowLator package.')
        # move into pkt_kg/libs/ directory
        cwd = os.getcwd()

        os.system(f"mkdir -p {loc}")
        os.chdir(loc)

        os.system(f'wget {owl_tools_url}')
        os.system('chmod +x owltools')

        # move back to the working directory
        os.chdir(cwd)

def download_owl(ulog: ubkgLogging, usource: ubkgSources, url: str, sab: str, loc: str, working_file: str) -> None:
    """
    Downloads an OWL file from a URL to a path on the local machine.
    :param ulog: logging object
    :param usource: source object (interface to sources.json)
    :param url: OWL URL
    :param sab: OWL sab
    :param loc: download path
    :param working_file:
    :return:
    """
    print_divider(ulog=ulog)

    ulog.print_and_logger_info(f'Downloading via wget')
    ulog.print_and_logger_info(f' * from: \'{url}\'')
    ulog.print_and_logger_info(f' * to: \'{loc}\'')

    cwd: str = os.getcwd()
    os.system(f"mkdir -p {loc}")
    os.chdir(loc)

    dicthist = usource.get(sab=sab, key='download_history')
    if dicthist:
        histsize = dicthist.get('size_mb', 'unknown')
        histtime = dicthist.get('max_time_minutes', 'unknown')
        minutes = "minute"
        if int(histtime) > 1:
            minutes = "minutes"
    ulog.print_and_logger_warning(
        f'The OWL file for {sab} has historically been <{histsize} MB and required <{histtime} {minutes} to download.')

    # Download via wget.
    print_divider(ulog=ulog)
    ulog.print_and_logger_info(f'WGET START')

    utimer=UbkgTimer(display_msg="Downloading")
    wgetResults: bytes = subprocess.check_output([f'wget {url}'], shell=True, stderr=subprocess.STDOUT)
    wgetResults_str: str = wgetResults.decode('utf-8')
    utimer.stop()

    # Validate result of download.
    for line in wgetResults_str.strip().split('\n'):
        if 'Length: unspecified' in line:
            ulog.print_and_logger_error(f'Failed to download {url}')
            ulog.print_and_logger_error(wgetResults_str)
            exit(1)

    # wget summary is the last block after a blank line
    summary: str = wgetResults_str.strip().split('\n\n')[-1]

    ulog.print_and_logger_info(summary)
    ulog.print_and_logger_info('WGET COMPLETE')

    print_divider(ulog=ulog)

    working_file = working_file.split('&download_format')[0]
    md5: str = hashlib.md5(open(working_file, 'rb').read()).hexdigest()
    md5_file: str = f'{working_file}.md5'
    ulog.print_and_logger_info(f'MD5 for owl file {md5} saved to {md5_file}')
    with open(md5_file, 'w', newline='') as fp:
        fp.write(md5)

    os.chdir(cwd)

def compare_file_md5(working_file: str) -> bool:
    if not os.path.isfile(working_file):
        return False
    md5_file: str = f'{working_file}.md5'
    if not os.path.isfile(md5_file):
        return False
    with open(md5_file, 'r', newline='') as fp:
        saved_md5 = fp.read()
        md5: str = hashlib.md5(open(working_file, 'rb').read()).hexdigest()
        if md5 == saved_md5:
            return True
    return False

# https://docs.python.org/3/library/xml.etree.elementtree.html#parsing-xml-with-namespaces
def scan_xml_tree_for_imports(tree: etree.ElementTree) -> list:
    # These should be read from the source file via the 'xmlns' property....
    owl_xmlns: str = 'http://www.w3.org/2002/07/owl#'
    rdf_xmlns: str = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'

    imports: list = tree.findall('owl:Ontology/owl:imports', namespaces={'owl': owl_xmlns})
    resource_uris: list = []
    for i in imports:
        resource_uri: str = i.get(f"{{{rdf_xmlns}}}resource")
        resource_uris.append(resource_uri)
    return resource_uris


def search_owl_file_for_imports(ulog: ubkgLogging, args: argparse.Namespace, owl_filename: str) -> None:

    """
    OWL files with imports must be pre-processed with the OBO Robot
    application.
    Checks whether the file has imports and terminates processing if
    these imports were not expected.

    :param ulog: logging object
    :param args: command line arguments
    :param owl_filename: file to check
    :return:
    """

    parser = etree.HTMLParser()
    tree: etree.ElementTree = etree.parse(owl_filename, parser)
    imports: list = scan_xml_tree_for_imports(tree)
    if len(imports) != 0:
        ulog.print_and_logger_info(f"The following imports were found in the OWL file {owl_filename} : {', '.join(imports)}")
        if args.with_imports is not True:
            ulog.print_and_logger_info(f"Imports found in OWL file {owl_filename}. Exiting")
            exit(1)
    else:
        ulog.print_and_logger_info(f"No imports were found in OWL file {owl_filename}")


def log_files_and_sizes(ulog: ubkgLogging, procdir: str) -> None:
    """
    Log the files that were produced in a processing directory.
    :param ulog: log
    :param procdir: processing directory
    :return:
    """
    print('')
    print_divider(ulog=ulog)
    for file in os.listdir(procdir):
        generated_file: str = os.path.join(procdir, file)
        size: int = os.path.getsize(generated_file)
        ulog.print_and_logger_info(f"Generated file '{generated_file}' size {size:,}")


def look_for_none_in_node_metadata_file(ulog: ubkgLogging, procdir: str) -> None:

    """
    Checks the nodes file for instances of "None".
    :param ulog: logging object
    :param procdir: output directory
    :return:
    """
    file: str = procdir + os.path.sep + 'OWLNETS_node_metadata.txt'
    print_divider(ulog=ulog)
    ulog.print_and_logger_info(f'Searching {file} for instances of "None"')

    # JAS 16 MAR 2023
    # Added parameters:
    #   engine='python' - use the Python parser engine instead of the C parser engine.
    #   on_bad_lines='skip'
    # This accounts for nodes files with erroneous characters, such as EOFs
    # (https://www.shanelynn.ie/pandas-csv-error-error-tokenizing-data-c-error-eof-inside-string-starting-at-line/)
    # Use case that occasioned this change: GLYCORDF

    data = pd.read_csv(file, sep='\t', engine='python', on_bad_lines='skip')

    # Original note:
    # "We will potentially find ontologies without synonyms."
    # However, none yet.
    message: str = f"Total columns in {file}: {len(data['node_synonyms'])}"
    ulog.print_and_logger_info(message)

    node_synonyms_not_None = data[data['node_synonyms'].str.contains('None') == False]
    message: str = f"Columns in {file} where node_synonyms is not None: {len(node_synonyms_not_None)}"
    ulog.print_and_logger_info(message)

    node_dbxrefs_not_None = data[data['node_dbxrefs'].str.contains('None') == False]
    message: str = f"Columns in {file} where node_dbxrefs is not None: {len(node_dbxrefs_not_None)}"
    ulog.print_and_logger_info(message)

    both_not_None = node_synonyms_not_None[node_synonyms_not_None['node_dbxrefs'].str.contains('None') == False]
    ulog.print_and_logger_info(f"Columns where node_synonyms && node_dbxrefs is not None: {len(both_not_None)}")


def robot_merge(ulog: ubkgLogging, owl_url: str) -> None:

    """
    ROBOT is a tool from OBO used to work with OWL files.
    It is provided as a JAR file and executed from the
    command line.
    """

    ulog.print_and_logger_info(f"Running robot merge on '{owl_url}'")
    loc = f'.{os.sep}robot'
    robot_jar = 'https://github.com/ontodev/robot/releases/download/v1.8.1/robot.jar'
    robot_sh = 'https://raw.githubusercontent.com/ontodev/robot/master/bin/robot'

    if 'JAVA_HOME' not in os.environ:
        print('The environment variable JAVA_HOME must be set and point to a valid JDK')
        exit(1)
    java_home: str = os.getenv('JAVA_HOME')
    jdk: str = file_from_path(java_home)
    if not re.match(r'^jdk-.*\.jdk$', jdk):
        ulog.print_and_logger_error(f'Environment variable JAVA_HOME={java_home} does not appear to point to a valid JDK.')
        exit(1)

    cwd = os.getcwd()
    os.system(f"mkdir -p {loc}")
    os.chdir(loc)

    if not os.path.exists(file_from_uri(robot_jar)):
        os.system(f"wget {robot_jar}")

    robot: str = file_from_uri(robot_sh)
    if not os.path.exists(robot):
        os.system(f"wget {robot_sh}")
        os.system(f"chmod +x {robot}")

    owl_file: str = file_from_uri(owl_url)
    if not os.path.exists(owl_file):
        os.system(f"wget {owl_url}")

    # https://robot.obolibrary.org/merge
    os.system(f".{os.sep}robot merge --input .{os.sep}{owl_file} --output .{os.sep}{owl_file}.merge")

    # move back to the working directory
    os.chdir(cwd)

def get_owl_file(ulog: ubkgLogging, uextract: ubkgExtract, owl_dir: str, args: argparse.Namespace, usource: ubkgSources) -> str:
    """
    Downloads and prepares the file specified by the owl_url argument.

    In many cases, the owl_url is actually not for the OWL file itself,
    but for an intermediate file that first must be processed to obtain
    the OWL file.

    :param ulog: logging object
    :param uextract: extraction object for the case of GZipped archives
    :param owl_dir: download directory for the OWL files
    :param args: command line arguments
    :param usource: sources object (interface to sources.json)
    :return: the path to the prepared OWL file.

    """

    print_divider(ulog=ulog)
    ulog.print_and_logger_info('Downloading and processing the OWL file.')

    # Obtain the file specified by the OWL URL. This may be an
    # intermediate file.
    working_file: str = file_from_uri(args.owl_url)

    # The file will be downloaded to the appropriate directory.
    owl_file: str = os.path.join(owl_dir, working_file)
    # Download the working file.

    download_owl(ulog=ulog, usource=usource, url=args.owl_url, sab=args.owl_sab, loc=owl_dir, working_file=working_file)

    #if args.force_owl_download is True or os.path.exists(owl_file) is False:
        #ulog.print_and_logger_info("Force download of OWL file specified.")
        #download_owl(ulog=ulog, url=args.owl_url, loc=owl_dir, working_file=working_file)
    #elif args.ignore_owl_md5 is True:
        #if args.verbose:
            #ulog.print_and_logger_info(f"Ignoring .owl file {owl_file} MD5")
    #elif not compare_file_md5(working_file=owl_file):
        #if args.verbose:
            #ulog.print_and_logger_info(f"MD5 of {working_file} does not match MD5 of {owl_file}: downloading.")
        #download_owl(ulog=ulog, url=args.owl_url, loc=owl_dir, working_file=working_file)

    """
    
    HANDLE FILES WITHOUT EXTENSIONS.
    Some download URLs (e.g., many from NCBO BioPortal) are REST calls that result in file names like
    download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb. The resulting downloaded file does not have
    a recognized OWL extension.

    If the downloaded OWL file does not have an extension, rename it to the default: <SAB>.owl
    April 2026: Trim extraneous download_format--e.g, &download_format=xxx
    """

    working_file = working_file.split('&download_format')[0]

    if '.' not in working_file[len(working_file) - 5:len(working_file)]:
        # No extension. Rename the downloaded file.
        working_file_new = args.owl_sab + '.OWL'
        ulog.print_and_logger_warning(f'The downloaded file does not have an extension. Renaming from {working_file} to {working_file_new}')
        os.system(f"mv {os.path.join(owl_dir, working_file)} {os.path.join(owl_dir, working_file_new)}")
        working_file = working_file_new
        owl_file = os.path.join(owl_dir, working_file_new)

    """
    HANDLE GZIPPED OWL FILES.
    In at least one use case (CHEBI), the downloaded file is a GZip archive.
    If the downloaded OWL file is GZipped, expand it.
    """

    # To identify Gzip files, check the first two bytes of the file: those of a GZip file are 1f:8b.
    filetest = open(os.path.join(owl_dir, working_file), mode='rb')
    if filetest.read(2) == b'\x1f\x8b':
        ulog.print_and_logger_info(f'{working_file} is a GZip archive.')

        # If the archive does not have a .gz file extension, add one so that the expansion will not overwrite it.
        # The expansion will write to a file with the original name.

        archive = working_file[working_file.rfind('/') + 1:]
        archive_extension = archive[archive.rfind('.'):len(archive)]
        if archive_extension.lower() != '.gz':
            ulog.print_and_logger_info(f'Adding gz extension to {working_file}.')
            # Append .gz to the downloaded file.
            working_file_gz = working_file + '.gz'
            os.system(f"mv {os.path.join(owl_dir, working_file)} {os.path.join(owl_dir, working_file_gz)}")

        else:
            working_file_gz = working_file

        # The extract_from_gzip will expand to a file with the same name, but minus the .gz extension.
        # e.g., ABC.OWL.gz -> ABC.OWL
        # ABC.OWL that's actually a GZip -> ABC.OWL.GZ_> ABC.OWL
        ulog.print_and_logger_error(f'Expanding {working_file_gz}')
        fileexpand = uextract.extract_from_gzip(zipfilename=os.path.join(owl_dir, working_file_gz), outputpath=owl_dir,
                                                outfilename='')
        working_file = str(fileexpand).split('/')[-1]
        owl_file = os.path.join(owl_dir, working_file)

        ulog.print_and_logger_info(f"Processed OWL file: {owl_file}")

    return owl_file

def find_existing_owl_file(ulog: ubkgLogging, owl_dir: str, sab:str) -> str:
    """
    Identifies a candidate for an "OWL file" generated in a previous run of the script,
    which would have called get_owl_file.

    Possibilities for OWL files are:
    1. a file with OWL extension, downloaded directly from source
    2. a file with OWL extension, named by the script after processing
       an intermediate file in cases:
       a. a download URL without an extension
       b. a GZipped file that was expanded
       c. a Turtle file that was re-serialized in XML/RDF format
    3. a file with a RDF extension

    :param ulog: logging object
    :param owl_dir: OWL directory
    :param sab: OWL sab
    :return: filename for candidate OWL file
    """

    print_divider(ulog=ulog)
    ulog.print_and_logger_info(f'Searching for an existing OWL file in {owl_dir}.')

    owl_files: list = glob.glob(os.path.join(owl_dir, '*.owl'))
    rdf_files: list = glob.glob(os.path.join(owl_dir, '*.rdf'))

    if len(owl_files) > 1:
        raise ValueError(f'Multiple OWL files found in {owl_dir}: {owl_files}')
    if owl_files:
        return owl_files[0]

    if len(rdf_files) > 1:
        raise ValueError(f'Multiple RDF files found in {owl_dir}: {rdf_files}')
    if rdf_files:
        return rdf_files[0]

    raise ValueError(f'No OWL or RDF files found in {owl_dir}')


def print_divider(ulog: ubkgLogging):
    print('')
    ulog.print_and_logger_info('-----------------------')

def get_rdf_graph(ulog: ubkgLogging, owl_dir: str, owl_file: str) -> Graph:
    """
    Converts an OWL file into a rdflib Graph object.
    :param ulog: logging object
    :param owl_dir: OWL directory
    :param owl_file: input OWL file name
    :return: rdflib Graph object
    """

    """
    The original logic (pre-2023) assumed that OWL files were in 
    RDF/XML format. Almost all the OWL files that had been 
    encountered up to January 2023 had been in RDF/XML, with the
    exception of GlycoRDF, which is available in both RDF/XML 
    and OWL/XML serializations.

    Howver, some ontologies are available only in non-RDF/XML 
    serializations--in particular, Turtle. 
    (Known case:GlycoCoO)

    The graph algorithm is currently: 
    1. Try to parse the OWL file as RDF/XML.
    2. If parsing fails, 
       a. Attempt to parse as Turtle.
       b. Serialize to RDF/XML.
       c. Reparse the RDF/XML.

    This script does not handle the case of an OWL file being in a 
    format other than RDF/XML or Turtle--in particular, OWL/RDF or RDF.
    There are currently no cases of desired OWL files that are 
    not in either RDF/XML or Turtle.

    For these other formats, the file will need to be converted first to
    either RDF/XML or Turtle. If the script needs to handle the file directly,
    an option might be to check for OWL/XML and reserialize as RDF/XML 
    per this forum discussion:
    https://github.com/RDFLib/rdflib/discussions/1571

    """

    print_divider(ulog=ulog)
    ulog.print_and_logger_info(f'Generating rdflib Graph of OWL file: {owl_file}')

    # Try to parse as XML.
    try:
        utimer = UbkgTimer(display_msg="Parsing")
        graph = Graph().parse(owl_file, format='xml')
        utimer.stop()
        ulog.print_and_logger_info(f'Successfully parsed {owl_file} as RDF/XML.')

    except:

        utimer.stop()
        """
        If the file is not in RDF/XML, the exception will be from xml (ExpatError), 
        not rdflib. Exception handling does not seem able to catch this lower-level error, 
        so use the generic exception handler. 

        The risk here, of course, is that the error is not from ExpatError.
        """

        ulog.print_and_logger_warning(f'Error parsing {owl_file} as RDF/XML.')
        ulog.print_and_logger_warning('This script currently supports only RDF/XML or Turtle.')

        ulog.print_and_logger_info('Attempting to parse as Turtle.')
        utimer = UbkgTimer(display_msg="Parsing")
        graph = Graph().parse(owl_file, format='ttl')
        utimer.stop()
        ulog.print_and_logger_info(f'Successfully parsed {owl_file} as Turtle.')

        convertedpath = os.path.join(owl_dir, 'converted.owl')

        ulog.print_and_logger_info(f'Serializing {owl_file} to RDF/XML format in file {convertedpath}')
        v = graph.serialize(format='xml', destination=convertedpath)

        utimer = UbkgTimer(display_msg="Parsing")
        graph2 = Graph().parse(convertedpath, format='xml')
        utimer.stop()
        ulog.print_and_logger_info(f'Successfully parsed {convertedpath} as RDF/XML.')

        graph = graph2

    return graph

def get_entity_metadata(ulog: ubkgLogging, graph: Graph) -> dict:
    """
    Obtains entity metadata used by PheKnowLator from a rdflib Graph.

    PheKnowLator uses the content of the return of this function
    to build the tabular edge and node files of the OWLNETS translation.

    :param ulog: logging object
    :param graph: a rdflib Graph of data parsed from an OWL file
    :return: dict of entity metadata in a schema recognized by the
             PheKnowLator package
    """

    print_divider(ulog=ulog)
    ulog.print_and_logger_info('Adding node metadata from the graph.')

    ont_classes = pkt.utils.gets_ontology_classes(graph)
    ont_labels = {str(x[0]): str(x[2]) for x in list(graph.triples((None, RDFS.label, None)))}
    ont_synonyms = pkt.utils.gets_ontology_class_synonyms(graph)
    ont_dbxrefs = pkt.utils.gets_ontology_class_dbxrefs(graph)
    ont_defs = pkt.utils.gets_ontology_definitions(graph)

    ulog.print_and_logger_info(' * Adding class metadata to the master metadata dictionary.')
    entity_metadata = {'nodes': {}, 'relations': {}}
    for cls in tqdm(ont_classes):
        # Get class metadata - synonyms and dbxrefs
        syns = '|'.join([k for k, v in ont_synonyms[0].items() if str(cls) in v])
        dbxrefs = '|'.join([k for k, v in ont_dbxrefs[0].items() if str(cls) in v])

        # Extract metadata
        cls_path_last: str = str(cls).split('/')[-1]
        if '_' in cls_path_last:
            namespace_candidate = re.findall(r'^(.*?)(?=\W|_)', cls_path_last)
            if len(namespace_candidate) > 0:
                namespace: str = namespace_candidate[0].upper()
        else:
            namespace: str = str(cls).split('/')[2]

        # Update dict.
        # Original note: namespace can be undefined.
        # 2026 - namespace is actually not needed.
        entity_metadata['nodes'][str(cls)] = {
            'label': ont_labels[str(cls)] if str(cls) in ont_labels.keys() else 'None',
            'synonyms': syns if syns != '' else 'None',
            'dbxrefs': dbxrefs if dbxrefs != '' else 'None',
            'namespace': namespace,
            'definitions': str(ont_defs[cls]) if cls in ont_defs.keys() else 'None',
        }

    ont_objects = pkt.utils.gets_object_properties(graph)
    ulog.print_and_logger_info(' * Adding object metadata to the master metadata dictionary.')
    for obj in tqdm(ont_objects):
        # get object label
        label_hits = list(graph.objects(obj, RDFS.label))
        label = str(label_hits[0]) if len(label_hits) > 0 else 'None'

        # Get object namespace.
        if 'obo' in str(obj) and len(str(obj).split('/')) > 5:
            namespace = str(obj).split('/')[-2].upper()
        else:
            if '_' in str(obj):
                namespace = re.findall(r'^(.*?)(?=\W|_)', str(obj).split('/')[-1])[0].upper()
            else:
                namespace = str(obj).split('/')[2]

        # Update dict
        entity_metadata['relations'][str(obj)] = {'label': label, 'namespace': namespace,
                                                  'definitions': str(
                                                      ont_defs[obj]) if obj in ont_defs.keys() else 'None'}

    ulog.print_and_logger_info(' * Adding RDF:type and RDFS:subclassOf.')
    entity_metadata['relations']['http://www.w3.org/2000/01/rdf-schema#subClassOf'] = \
        {'label': 'subClassOf', 'definitions': 'None', 'namespace': 'www.w3.org'}
    entity_metadata['relations']['http://www.w3.org/1999/02/22-rdf-syntax-ns#type'] = \
        {'label': 'type', 'definitions': 'None', 'namespace': 'www.w3.org'}

    print_divider(ulog=ulog)
    ulog.print_and_logger_info('Stats for original graph before running OWL-NETS:')
    pkt.utils.derives_graph_statistics(graph)

    return entity_metadata


def get_owlnets(ulog: ubkgLogging, graph: Graph, working_dir: str, args: argparse.Namespace) -> pkt.OwlNets:
    """
    Converts a rdflib Graph into a PheKnowLator OWLNETS object.
    :param ulog: logging object
    :param graph: rdflib Graph parsed from an OWL file
    :param working_dir: output directory
    :param args: command line arguments
    :return: PheKnowLator OWLNETS object
    """

    print_divider(ulog=ulog)
    ulog.print_and_logger_info('Converting rdflib graph to OWLNETS object.')
    ulog.print_and_logger_info(' * Initializing owlnets class.')

    """
    Arguments for the pkt.OwlNets class initialization:
    - graph: An RDFLib object or a list of RDFLib Graph objects
    - write_location: a file path used for writing knowledge graph data (e.g. "resources/")
    - filename: a string containing the filename for the full knowledge graph (e.g. "/hpo_owlnets")
    - kg_construct_approach: a string containing the type of construction approach used to build the knowledge graph
    - owl_tools: a string pointing to the location of the owl tools library
    Items that should be excluded from the cleaned graph:
    - top_level: a list of ontology namespaces that should not appear in any or in the clean graph
    - support: A list of ontology namespaces that should not appear in any or in the clean graph
    - relations: A list of ontology namespaces that should not appear in any or in the clean graph
    """
    owlnets = pkt.OwlNets(graph=graph,
                          write_location=working_dir + os.sep,
                          filename=file_from_uri(args.owl_url),
                          kg_construct_approach=None,
                          owl_tools=args.owltools_dir + os.sep + 'owltools',
                          # top_level=['ISO', 'SUMO', 'BFO'],
                          # support=['IAO', 'SWO', 'OBI', 'UBPROP'],
                          # relations=['RO'],
                          top_level=['ISO', 'SUMO', 'BFO'],
                          support=['IAO', 'SWO', 'UBPROP'],
                          relations=['RO']
                          )

    ulog.print_and_logger_info('* Removing disjointness with Axioms.')
    owlnets.removes_disjoint_with_axioms()

    ulog.print_and_logger_info('* Removing triples used only to support semantics.')
    cleaned_graph = owlnets.removes_edges_with_owl_semantics()
    filtered_triple_count = len(owlnets.owl_nets_dict['filtered_triples'])
    ulog.print_and_logger_info(
        '** Removed {} triples that were not biologically meaningful.'.format(filtered_triple_count))

    ulog.print_and_logger_info('* Gathering list of owl:Class and owl:Axiom entities.')
    owl_classes = list(pkt.utils.gets_ontology_classes(owlnets.graph))
    owl_axioms: list = []
    for x in tqdm(set(owlnets.graph.subjects(RDF.type, OWL.Axiom))):
        src = set(owlnets.graph.objects(list(owlnets.graph.objects(x, OWL.annotatedSource))[0], RDF.type))
        tgt = set(owlnets.graph.objects(list(owlnets.graph.objects(x, OWL.annotatedTarget))[0], RDF.type))
        if OWL.Class in src and OWL.Class in tgt:
            owl_axioms += [x]
        elif (OWL.Class in src and len(tgt) == 0) or (OWL.Class in tgt and len(src) == 0):
            owl_axioms += [x]
        else:
            pass
    node_list = list(set(owl_classes) | set(owl_axioms))
    ulog.print_and_logger_info(
        '** There are:\n-{} OWL:Class objects\n-{} OWL:Axiom Objects.'.format(len(owl_classes), len(owl_axioms)))

    ulog.print_and_logger_info('* Decoding owl semantics.')
    owlnets.cleans_owl_encoded_entities(node_list)
    decoded_graph: Dict = owlnets.gets_owlnets_graph()

    ulog.print_and_logger_info('* Updating graph to obtain cleaned edges.')
    owlnets.graph: Dict = cleaned_graph + decoded_graph

    str1 = 'Decoded {} owl-encoded classes and axioms. Note the following:\nPartially processed {} cardinality ' \
           'elements\nRemoved {} owl:disjointWith axioms\nIgnored:\n  -{} misc classes;\n  -{} classes constructed with ' \
           'owl:complementOf;\n  -{} classes containing negation (e.g. pr#lacks_part, cl#has_not_completed)\n' \
           '\nFiltering removed {} semantic support triples'
    stats_str = str1.format(
        len(owlnets.owl_nets_dict['decoded_entities'].keys()), len(owlnets.owl_nets_dict['cardinality'].keys()),
        len(owlnets.owl_nets_dict['disjointWith']), len(owlnets.owl_nets_dict['misc'].keys()),
        len(owlnets.owl_nets_dict['complementOf'].keys()), len(owlnets.owl_nets_dict['negation'].keys()),
        len(owlnets.owl_nets_dict['filtered_triples']))
    print('')

    ulog.print_and_logger_info(f'OWL-NETS results: {stats_str}')

    # Original note:
    # run line below if you want to ensure resulting graph contains
    # common_ancestor = 'http://purl.obolibrary.org/obo/BFO_0000001'
    # owlnets.graph = owlnets.makes_graph_connected(owlnets.graph, common_ancestor)

    ulog.print_and_logger_info(f"* Writing owl-nets results files to directory '{working_dir}'.")
    owlnets.write_location = working_dir
    owlnets.write_out_results(owlnets.graph)

    return owlnets

def write_edges_file(sab:str, ulog: ubkgLogging, ustand: ubkgStandardizer, working_dir: str, owlnets: pkt.OwlNets):
    """
    Writes an edges file in OWLNETS format.
    :param sab: SAB that is to be standardized
    :param ulog: logging object
    :param ustand: UBKG code standardizer object
    :param working_dir: output directory
    :param owlnets: OWLNets object
    :return:
    """

    print_divider(ulog=ulog)
    edge_list_filename: str = working_dir + os.sep + 'OWLNETS_edgelist.txt'
    ulog.print_and_logger_info(f"Write edge list results to '{edge_list_filename}'")

    # Standardize IRIs to SAB:code format.
    df = pd.DataFrame(owlnets.graph)
    df.columns = ['subject','predicate','object']
    df['subject_standardized'] = ustand.standardize_code(x=df['subject'], ingestSAB=sab)
    df['object_standardized'] = ustand.standardize_code(x=df['object'], ingestSAB=sab)

    # Simplify predicate IRIs to standardized predicate terms.
    df['predicate_standardized'] = ustand.identify_relationships(predicate=df['predicate'])

    df = df[['subject_standardized','predicate_standardized','object_standardized']]
    df.columns = ['subject','predicate','object']
    df.to_csv(edge_list_filename, index=False, header=True, encoding='utf-8', sep='\t')
    # Standardize code IRIs to SAB:Code format.

    #with open(edge_list_filename, 'w') as out:
        #out.write('subject' + '\t' + 'predicate' + '\t' + 'object' + '\n')
        #for row in tqdm(owlnets.graph):
            #out.write(str(row[0]) + '\t' + str(row[1]) + '\t' + str(row[2]) + '\n')


def write_nodes_file(ulog: ubkgLogging, working_dir: str, owlnets: pkt.OwlNets, entity_metadata: dict):
    """
    Writes an edges file in OWLNETS format.
    :param ulog: logging object
    :param working_dir: output directory
    :param owlnets: OWLNets object
    :param entity_metadata: OWLNets entity metadata
    :return:
    """
    print_divider(ulog=ulog)
    ulog.print_and_logger_info(f'Writing nodes results to {working_dir}.')

    ulog.print_and_logger_info('Getting unique nodes from OWL-NETS graph')
    nodes = set([x for y in [[str(x[0]), str(x[2])] for x in owlnets.graph] for x in y])

    node_metadata_filename: str = working_dir + os.sep + 'OWLNETS_node_metadata.txt'
    ulog.print_and_logger_info(f"Write node metadata results to '{node_metadata_filename}'")
    with open(node_metadata_filename, 'w') as out:
        out.write('node_id' + '\t' + 'node_namespace' + '\t'
                  + 'node_label' + '\t' + 'node_definition'
                  + '\t' + 'node_synonyms' + '\t'
                  + 'node_dbxrefs' + '\n')

        for x in tqdm(nodes):
            if x in entity_metadata['nodes'].keys():
                namespace = entity_metadata['nodes'][x]['namespace']
                labels = entity_metadata['nodes'][x]['label']
                definitions = entity_metadata['nodes'][x]['definitions']
                synonyms = entity_metadata['nodes'][x]['synonyms']
                dbxrefs = entity_metadata['nodes'][x]['dbxrefs']

                out.write(x + '\t' + namespace + '\t' + labels +
                          '\t' + definitions + '\t' + synonyms +
                          '\t' + dbxrefs + '\n')

def remove_old_files(ulog: ubkgLogging, args: argparse.Namespace):
    """
    Removes prior versions of files, depending on command line arguments.
    :param ulog: ubkgLogging object
    :param args: command line arguments
    """
    cwd = os.getcwd()
    print_divider(ulog=ulog)

    # 1. Remove old output--i.e., edge and node files
    output_dir = os.path.join(args.owlnets_dir, args.owl_sab)
    ulog.print_and_logger_warning(f"Deleting prior output files in working directory {output_dir}")
    os.chdir(output_dir)
    os.system("rm -f *")

    # 2. If a new fetch of the OWL file was requested, remove old
    #    OWL files.
    owl_dir = os.path.join(args.owl_dir, args.owl_sab)
    if args.force_owl_download:
        ulog.print_and_logger_warning(f'Deleting prior versions of files in {owl_dir}')
        os.chdir(owl_dir)
        os.system("rm -f *.owl *.md5 *.ttl *.rdf *download*")

    os.chdir(cwd)

def main():

    # ------
    # SET UP
    # ------

    # Set up centralized logging.
    repo_root = find_repo_root()
    log_dir = os.path.join(repo_root, 'generation_framework/builds/logs')
    ulog = ubkgLogging(log_dir=log_dir, log_file='pheknowlator.log')

    # Process and document command line arguments.
    args = get_args(ulog=ulog)

    # Initialize extractor object, used to handle the case in which the
    # downloaded OWL file is a compressed file that must be expanded.
    uextract = ubkgExtract(ulog=ulog)

    # Initialize the code standardizer object.
    ustandardizer = ubkgStandardizer(ulog=ulog, repo_root=repo_root)

    # Obtain application configuration.
    cfg = ubkgConfigParser(path='ubkgjkg.ini', ulog=ulog)

    # Read and validate the file of SAB-specific configuration.
    usource = ubkgSources(ulog=ulog, cfg=cfg, repo_root=repo_root)

    # Housekeeping - remove prior versions of input or output files.
    remove_old_files(ulog=ulog, args=args)

    # Start the processing timer.
    start_time = time.time()

    """
    The following is possibly a historical concern.
    
    The Pheknowlator package cannot (or could not?) work with OWL files 
    that have imports. (However, there was also a --with_imports 
    argument, so perhaps it is not an issue.)
    
    OWL files with imports should be pre-processed by OBO's 
    Robot application.
    
    The script originally allowed for a two-step workflow:
    1. Run the script on a file with imports to create a merged OWL file.
    2. Run the script on the merged OWL file.
    
    Imports have not been an issue with the OWL files that are used
    for the UBKG. The two-step workflow has not been necessary, at least
    since 2023.
    
    The functionality is retained for potential future use cases.
    
    """
    #if args.robot is True:
        #robot_merge(ulog=ulog, owl_url=args.owl_url)
        #elapsed_time = time.time() - start_time
        #ulog.print_and_logger_info('Done! Elapsed time %s', "{:0>8}".format(str(timedelta(seconds=elapsed_time))))
        #exit(0)

    # Download JARs related to the PheKnowlator package.
    download_owltools(ulog=ulog, loc=args.owltools_dir)

    # Determine path for outputs of processing, based on the SAB.
    working_dir: str = os.path.join(args.owlnets_dir, args.owl_sab)
    ulog.print_and_logger_info(f"Creating output directory {working_dir}", )
    os.system(f"mkdir -p {working_dir}")

    # ------
    # OBTAIN THE OWL FILE.
    # ------

    """
    Although it is possible to work with an OWL file directly in memory,
    there is a risk of issues obtaining the file, such as timeout or 
    loss of connectivity, especially for large OWL files. (CHEBI used to be an issue, but 
    is now available as a compressed GZip.)
    """

    # Directory containing OWL files related to the SAB.
    owl_dir: str = os.path.join(args.owl_dir, args.owl_sab)

    if args.force_owl_download is True:
        # Download the file and process it as necessary (e.g., expanding if an archive).
        owl_file = get_owl_file(ulog=ulog, uextract=uextract, owl_dir=owl_dir, args=args, usource=usource)
        # Defensive: verify that the file has no unexpected imports.
        search_owl_file_for_imports(ulog=ulog, args=args, owl_filename=owl_file)
    else:
        # Look for an OWL file obtained during a prior run of this script.
        owl_file = find_existing_owl_file(ulog=ulog, owl_dir=owl_dir, sab=args.owl_sab)

    # ------
    # CONVERT THE OWL FILE INTO A rdflib GRAPH.
    # ------
    graph = get_rdf_graph(ulog=ulog, owl_dir=owl_dir, owl_file=owl_file)

    # ------
    # EXTRACT ENTITY METADATA FROM GRAPH.
    entity_metadata = get_entity_metadata(ulog=ulog, graph=graph)

    # ------
    # CONVERT GRAPH TO OWLNETS FORMAT.
    # ------
    owlnets = get_owlnets(ulog=ulog, graph=graph,
                          working_dir=working_dir, args=args)

    # ------
    # WRITE OUTPUT
    # ------
    write_edges_file(sab=args.owl_sab, ulog=ulog, ustand=ustandardizer, working_dir=working_dir, owlnets=owlnets)
    write_nodes_file(ulog=ulog, working_dir=working_dir, owlnets=owlnets, entity_metadata=entity_metadata)

    log_files_and_sizes(ulog=ulog, procdir=working_dir)

    # look_for_none_in_node_metadata_file(ulog=ulog, procdir=working_dir)

    # Add log entry for processing time.
    elapsed_time = time.time() - start_time
    ulog.print_and_logger_info(
        f'OWL conversion completed. Elapsed time:  {"{:0>8}".format(str(timedelta(seconds=elapsed_time)))}')


if __name__ == "__main__":
    main()