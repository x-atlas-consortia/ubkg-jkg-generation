#!/usr/bin/env python
# coding: utf-8

# UBKG functions for working with configuration files.

from configparser import ConfigParser,ExtendedInterpolation
import configparser
import os

# Centralized logging
from ubkg_utilities.ubkg_logging import UbkgLogging

class ubkgConfigParser:

    def __init__(self, path: str, repo_root: str, case_sensitive: bool = False):
        """
        Read and validate the specified configuration file
        :param path: full path to the configuration file
        :param repo_root: root directory of the repository
        :param case_sensitive: whether the configuration file should be case-sensitive
        """

        self.ulog = UbkgLogging(repo_root=repo_root)

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