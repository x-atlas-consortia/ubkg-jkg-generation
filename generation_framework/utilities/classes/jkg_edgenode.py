"""
jkg_edgenode.py
Class that manages the edge and node files of a SAB.
"""
import os
import polars as pl
from tqdm import tqdm

# Centralized logging
from .ubkg_logging import ubkgLogging
# Application configuration object
from .ubkg_config import ubkgConfigParser
# Spinner to wrap block functions in tqdm
from .ubkg_timer import UbkgTimer

from ..functions.find_repo_root import find_repo_root

class Jkgedgenode:

    def _load_file(self, filetype: str) ->pl.DataFrame:
        """
        Loads an edge or node file into a Polars DataFrame.
        :param filetype: edge or node filetype
        """
        return


    def __init__(self, log: ubkgLogging, cfg: ubkgConfigParser, sab: str):

        self.log = log
        self.cfg = cfg
        self.sab = sab

        # Get the path to the directory that contains the edge and node files.
        self.jkg_dir = cfg.get_value(section='directories', key='sab_jkg_dir')
        self.jkg_path = os.path.join(find_repo_root(),self.jkg_dir, sab)

        #self.jkg_nodes = self._load_file(filetype='node')