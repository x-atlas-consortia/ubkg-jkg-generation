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

class ubkgLogging:

    def __init__(self, log_dir: str, log_file:str):

        """
        :param log_dir: logging directory
        :param log_file: logging file
        """

        #log_dir = os.path.join(repo_root,'generation_framework/builds/logs')
        #log_file = 'ubkg.log'
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