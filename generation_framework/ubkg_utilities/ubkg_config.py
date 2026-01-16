#!/usr/bin/env python
# coding: utf-8

# UBKG functions for working with configuration files.

from configparser import ConfigParser,ExtendedInterpolation
import configparser
import os
import sys

# The following allows for an absolute import from an adjacent script directory--i.e., up and over instead of down.
# Find the absolute path. (This assumes that this script is being called from build_csv.py.)
fpath = os.path.dirname(os.getcwd())
fpath = os.path.join(fpath, 'generation_framework/ubkg_utilities')
sys.path.append(fpath)
# Centralized logging
from ubkg_logging import ubkgLogging

class ubkgConfigParser:

    def __init__(self, path: str, log_dir: str, log_file: str, case_sensitive: bool = False):
        """
        Read and validate the specified configuration file
        :param path: full path to the configuration file
        :param log_dir: logging directory
        :param log_file: logging file
        :param case_sensitive: whether the configuration file should be case-sensitive
        """

        self.ulog = ubkgLogging(log_dir=log_dir, log_file=log_file)

        self.config = ConfigParser(interpolation=ExtendedInterpolation())
        if case_sensitive:
            # Force case sensitivity.
            self.config.optionxform = str

        if not os.path.exists(path):
            self.ulog.print_and_logger_error(f'Missing configuration file: {path}')
            exit(1)

        try:
            self.config.read(path)
        except configparser.ParsingError as e:
            self.ulog.print_and_logger_error(f'Error parsing config file {path}')
            exit(1)

        self.ulog.print_and_logger_info(f'Config file found at {path}')

    def get_value(self,section: str, key:str)-> str:

        # Searches a configuration file for the value that corresponds to [section][key].
        try:
            return self.config[section][key]
        except KeyError as e:
            self.ulog.print_and_logger_error(f'Error reading configuration file: Missing key [{key}] in section [{section}]')
            exit(1)

    def get_section(self, section: str)-> dict:

        # Returns a section of the config file as a dictionary.

        dictreturn = {}

        try:
            sect = self.config[section]
            for key in sect:
                dictreturn[key]=self.config[section][key]
        except configparser.NoSectionError as e:
            self.ulog.print_and_logger_error(f'Error reading configuration file: Missing section [{section}]')
            exit(1)

        return dictreturn