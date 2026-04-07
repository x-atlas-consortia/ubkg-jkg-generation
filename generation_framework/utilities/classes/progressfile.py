"""
ProgressFile
Helper class that wraps an ijson streaming read with a tqdm progress bar.
"""
from typing import IO
from tqdm import tqdm

class ProgressFile:
    def __init__(self, f: IO[bytes], pbar: tqdm):
        """
        :param f: ijson streaming read file
        :param pbar: tqdm progress bar
        """
        self.f = f
        self.pbar = pbar

    def read(self, size=-1):
        """
        Wrapper of ijson's read function to update a tqdm progress bar.
        :param size: number of bytes to read
        """
        # Call ijson's read method.
        chunk = self.f.read(size)
        # Update the tqdm progress bar.
        self.pbar.update(len(chunk))
        return chunk