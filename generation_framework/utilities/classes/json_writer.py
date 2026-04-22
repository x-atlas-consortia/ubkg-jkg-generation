#!/usr/bin/env python
# coding: utf-8
"""
Class that writes to a JSON file, with a TQDM progress bar.

Format options:

- pretty-printing with spacing, indentation, and line feeds for legibility
- minimal (but not no) spacing, line feeds, and indentation

The difference in file size resulting from large numbers of data elements can
be significant for the two format options.
"""

import json
import textwrap
from tqdm import tqdm

class JsonWriter:

    def __init__(self, outpath: str, pretty: bool=False, indent: int=4):
        # Path to JSON output file
        self.outpath = outpath
        # Whether to "pretty print" or write with minimal spacing and indentation
        self.write_pretty = pretty == "true"

        # Indentation spacing if pretty printing
        self.indent_spaces  = int(indent)
        self.node_indent = " " * self.indent_spaces
        self.top_indent = " " * (self.indent_spaces // 2)

    def start_json(self):
        """
        Starts a JSON file with a open bracket.

        """
        with open(self.outpath, 'w', encoding="utf-8") as f:
            f.write('{\n')

    def end_json(self):
        """
        Ends a JSON file with a close bracket.

        """
        with open(self.outpath, 'a', encoding="utf-8") as f:
            f.write('\n}')

    def start_list(self, keyname: str, mode:str = "a"):
        """
        Starts a list in the JSON file.
        :param keyname: name of the key for which the list is the value
        :param mode: w (write) or a (append, default)

        The use case is: write {"keyname":[list of elements]}

        """
        print(f'Starting {keyname} array...')
        with open(self.outpath, mode, encoding="utf-8") as f:
            f.write(self.top_indent + '"' + keyname + '":' + self.top_indent + '[\n')

    def end_list(self):
        """
        Ends a list in the JSON file.

        """
        with open(self.outpath, "a", encoding="utf-8") as f:
            # Close array, using top indentation.
            f.write("\n" + self.top_indent + "]")

    def write_list(self, list_name: str, list_content: list):
        """
        Writes a list to the JSON file as the value of a key, using
        a TQDM progress bar.
        :param list_content: list to write
        :param list_name: display name for the list

        The use case is: write {"keyname":[list of elements]}

        Assumptions:
        1. This routine is called between start_list and end_list.
        2. This routine assumes that list_content contains JSON-compliant strings.
        """

        # Because start_list was called, always append.
        with open(self.outpath, "a", encoding="utf-8") as f:

            for i, node in enumerate(tqdm(list_content, desc=f"Writing {list_name}...", total=len(list_content))):

                # Produce node JSON either as pretty (multi-line) or minimal (one-line, with minimal indentation)
                if isinstance(node, str):
                    node_indented = node.strip()
                else:
                    if self.write_pretty:
                        # Pretty-print the node.
                        node_json = json.dumps(node, ensure_ascii=False, indent=self.indent_spaces)
                        node_indented = textwrap.indent(node_json, self.node_indent)
                    else:
                        # Use minimal separator spacing, with a small indent.
                        node_json = json.dumps(node, ensure_ascii=False, separators=(',', ':'))
                        node_indented = self.node_indent + node_json

                # Write comma+newline before each node except the first
                if i:
                    f.write(",\n")

                f.write(node_indented)

    def write_line_feed(self):
        """
        Writes a line feed to the JSON file.
        """
        with open(self.outpath, 'a', encoding="utf-8") as f:

            f.write('\n')

    def write_comma(self):
        """
        Writes a comma to the JSON file.
        """
        with open(self.outpath, 'a', encoding="utf-8") as f:

            f.write(',')



