"""
jkg_edgenode.py
Class that manages the edge and node files of a SAB.
Works with files in either UBKG edge node or JKG edge node formats.
"""
import os
import sys
#import polars as pl
import pandas as pd
from tqdm import tqdm

from .ubkg_extract import ubkgExtract
# Centralized logging
from .ubkg_logging import ubkgLogging
# Application configuration object
from .ubkg_config import ubkgConfigParser
# Source file load manager
from .ubkg_extract import ubkgExtract

# The following allows for an absolute import from an adjacent script directory--i.e., up and over instead of down.
# Find the absolute path. (This assumes that this script is being called from build_csv.py.)
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/utilities')
sys.path.append(fpath)

from functions.find_repo_root import find_repo_root

class Jkgedgenode:

    def __init__(self, log: ubkgLogging, cfg: ubkgConfigParser, sab: str, filedir:str):

        """

        :param log: ubkgLogging object
        :param cfg: application config object
        :param sab: SAB
        :param filedir: path to edge and node files
        """

        # Prevent truncation, especially of columns with URLs.
        #pl.Config.set_fmt_str_lengths(200)

        self.log = log
        self.cfg = cfg
        self.sab = sab

        self.uextract = ubkgExtract(ulog=log)

        # Get the path to the directory that contains the edge and node files.
        #self.jkg_dir = cfg.get_value(section='Directories', key='sab_jkg_dir')
        self.filedir = filedir
        self.jkg_path = os.path.join(find_repo_root(),self.filedir)

        # Load the edge and node files.
        self.edges = self._load_file(filetype='edge')
        self.nodes = self._load_file(filetype='node')


    def get_filename(self, filetype: str) -> str:
        """
        Source files can be named in various ways. For example, the node file can be named:
        - OWLNETS_node_metadata.txt (the output of PheKnowLator)
        - nodes.tsv
        - nodes.txt
        etc.

        Determine which name the edge or node file uses.

        :param filetype: the type of node
        :return: the name of the edge or node file
        """
        if filetype == 'node':
            key = 'jkg_node'
        else:
            key = 'jkg_edge'

        file_names = self.cfg.get_list(section='jkg_out', key=key)

        for f in file_names:
            if os.path.exists(os.path.join(self.jkg_path, f)):
                return f

        # Error case: no file found with name in argument list.
        lfile = ','.join(str(f) for f in file_names)
        raise FileNotFoundError(f'No {filetype} file found with name in list: ' + lfile)

    def _load_file(self, filetype: str) ->pd.DataFrame:
        """
        Loads an edge file into a Pandas DataFrame.
        :param filetype: the type of file (edge or node)
        """

        filename = self.get_filename(filetype=filetype)
        filepath = os.path.join(self.jkg_path, filename)
        self.log.print_and_logger_info(f'Loading JKG EN {filetype} file: {filepath}')
        df= self.uextract.read_csv_with_progress_bar(path=filepath, sep='\t')
        return df

        # If using Polars instead of Pandas
        #return self.uextract.polars_scan_csv_with_timer(filename=nodefilepath, separator='\t')