#!/usr/bin/env python
# coding: utf-8

"""
UbkgLogging: manages custom, centralized Python logging.

Logs will be stored in the subdirectory 'logging' of the repo root.

Uses a custom logging.ini file, located in the logging directory,
to configure JSON logging.

"""
import logging.config
import os

class UbkgLogging:

    def _find_repo_root(self,start_dir=None)-> str:
        """
        Find the root of a GitHub repository by searching for a `.git` folder.

        :param start_dir: The directory to start the search from.
        Defaults to the current working directory.

        Returns: the path to the repository root, or None if not found.
        """
        # If no start directory is provided, use the current working directory
        if start_dir is None:
            start_dir = os.getcwd()

        current_dir = start_dir

        while True:
            # Check if `.git` exists in the current directory
            if os.path.isdir(os.path.join(current_dir, '.git')):
                return current_dir  # Found the repo root.

            # Move up one directory level
            parent_dir = os.path.dirname(current_dir)

            # If we reach the root directory, stop (no more parents to search)
            if current_dir == parent_dir:
                return None  # Repo root not found

            current_dir = parent_dir

    def __init__(self):

        # ------
        # Find the path to the log directory.
        repo_root =  self._find_repo_root()
        print('repo_root',repo_root)

        log_dir = os.path.join(repo_root,'logging')
        print('log_dir',log_dir)
        log_file = 'ubkg.log'
        self.logger = logging.getLogger(__name__)

        # logging.ini to configure custom, centralized Python logging.
        log_config = os.path.join(log_dir,'logging.ini')
        logging.config.fileConfig(log_config, disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log_file})

    def print_and_logger_info(self,message: str) -> None:
        print(message)
        self.logger.info(message)