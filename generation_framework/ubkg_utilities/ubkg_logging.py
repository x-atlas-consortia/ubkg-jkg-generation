#!/usr/bin/env python
# coding: utf-8

"""
UbkgLogging: manages custom, centralized Python logging.

Logs will be stored in the subdirectory named 'logging' of the repository root.

Uses a custom logging.ini file, located in the logging directory,
to configure JSON logging.

"""
import logging.config
import os

class UbkgLogging:

    def __init__(self, repo_root: str):

        """
        :param repo_root: repo root directory
        """

        log_dir = os.path.join(repo_root,'logging')
        log_file = 'ubkg.log'
        self.logger = logging.getLogger(__name__)

        # logging.ini to configure custom, centralized Python logging.
        log_config = os.path.join(log_dir,'logging.ini')
        logging.config.fileConfig(log_config, disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log_file})

    def print_and_logger_info(self,message: str) -> None:
        print(message)
        self.logger.info(message)

    def print_and_logger_error(self,message: str) -> None:
        print(message)
        self.logger.error(message)