"""
Class to manage the sources.json file.
"""

import os
import json
from typing import Any

# Common logging
from .ubkg_logging import ubkgLogging
# Configuration
from .ubkg_config import ubkgConfigParser

class ubkgSources:

    def __init__(self, ulog: ubkgLogging, cfg: ubkgConfigParser, repo_root: str):
        """
        Opens and reads the sources.json file.
        :param ulog: common logging object.
        :param cfg: configuration object.
        """

        self._ulog = ulog
        self._cfg = cfg
        self.repo_root = repo_root
        self._file_path = os.path.join(repo_root, self._cfg.get_value(section='sabs', key='sab_json_file'))

        # Read the sabs.json file.
        try:
            with open(self._file_path, 'r') as f:
                self.sab_json = json.load(f)
        except FileNotFoundError:
            self._ulog.print_and_logger_info(f'{self._file_path} not found.')
            exit(1)

    def get(self, sab: str, key: str) -> str:
        """
        Composite getter function.
        :param sab: SAB name.
        :param key: key in SAB object.
        """
        if sab not in self.sab_json.keys():
            self._ulog.print_and_logger_info(f'{sab} not found in {self._file_path}.')
            exit(1)
        sabobj = self.sab_json[sab]
        if key not in sabobj.keys():
            err=f"Key '{key}' not found in available keys {list(sabobj.keys())} for SAB '{sab}' in {self._file_path}"
            self._ulog.print_and_logger_info(err)
            raise KeyError(err)

        return self.sab_json[sab][key]

