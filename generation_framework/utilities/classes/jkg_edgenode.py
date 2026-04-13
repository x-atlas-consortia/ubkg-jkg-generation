"""
jkg_edgenode.py
Class that manages the edge and node files of a SAB.
"""
import os
import polars as pl
import pandas as pd
from tqdm import tqdm

from .ubkg_extract import ubkgExtract
# Centralized logging
from .ubkg_logging import ubkgLogging
# Application configuration object
from .ubkg_config import ubkgConfigParser
# Source file load manager
from .ubkg_extract import ubkgExtract

from ..functions.find_repo_root import find_repo_root

class Jkgedgenode:

    def __init__(self, log: ubkgLogging, cfg: ubkgConfigParser, sab: str):

        # Prevent truncation, especially of columns with URLs.
        pl.Config.set_fmt_str_lengths(200)

        self.log = log
        self.cfg = cfg
        self.sab = sab

        self.uextract = ubkgExtract(ulog=log)

        # Get the path to the directory that contains the edge and node files.
        self.jkg_dir = cfg.get_value(section='directories', key='sab_jkg_dir')
        self.jkg_path = os.path.join(find_repo_root(),self.jkg_dir, sab)

        # Load the node file.
        self.jkg_nodes = self._load_node_file()


    def _get_filename(self, filetype: str) -> str:
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
            key = 'nodefilenames'
        else:
            key = 'edgefilenames'

        file_names = self.cfg.get_list(section='edgenodefiles', key=key)

        for f in file_names:
            if os.path.exists(os.path.join(self.jkg_path, f)):
                return f

        # Error case: no file found with name in argument list.
        lfile = ','.join(str(f) for f in file_names)
        raise FileNotFoundError('No file found with name in list: ' + lfile)

    def _load_node_file(self) ->pd.DataFrame:
        """
        Loads a node file into a Pandas DataFrame.
        """

        nodefilename = self._get_filename(filetype='node')
        nodefilepath = os.path.join(self.jkg_path, nodefilename)
        return self.uextract.read_csv_with_progress_bar(path=nodefilepath, sep='\t')
        #return self.uextract.polars_scan_csv_with_timer(filename=nodefilepath, separator='\t')