#!/usr/bin/env python
# coding: utf-8

"""
UbkgTimer
Custom timer thread that displays a timer in its own thread.
The use case for the timer is for lazy functions like Polars's scan_csv.
"""

import threading
import time
from tqdm import tqdm

class UbkgTimer:

    def __init__(self, display_msg:str, refresh_interval:int=5) -> None:

        """
        :param display_msg: message to display in the timer
        :param refresh_interval: refresh interval in seconds
        """
        # Initiate the timer event.
        self.stop_ev = threading.Event()

        self.refresh_interval = int(refresh_interval)

        # Start a tqdm timer that will update the timer display.
        self.pbar = tqdm(total=0, bar_format="{desc}{postfix}", desc=f"{display_msg}", leave=True)

        self.thread = threading.Thread(target=self._timer_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """
        Stops the timer event and the tqdm display.
        :return:
        """
        self.stop_ev.set()
        self.thread.join(timeout=1.0)
        self.pbar.close()

    def _fmt_elapsed(self, seconds: float) -> str:
            """
            Formats elapsed time as a human-readable string.
            :param seconds: elapsed time
            :return: formatted string
            """
            s = int(seconds)
            h, s = divmod(s, 3600)
            m, s = divmod(s, 60)
            if h:
                return f"{h:d}:{m:02d}:{s:02d}"
            return f"{m:02d}:{s:02d}"

    def _timer_loop(self):

        """
        Event thread loop that updates the content used by the
        tqdm timer.
        """

        # Get the start time for the loop.
        start = time.perf_counter()

        # Update the display until the calling function stops the event.
        while not self.stop_ev.is_set():

            elapsed = time.perf_counter() - start
            self.pbar.set_postfix_str(f"elapsed = {self._fmt_elapsed(elapsed)}")
            self.pbar.refresh()
            time.sleep(self.refresh_interval)

        # final update before exit
        elapsed = time.perf_counter() - start
        self.pbar.set_postfix_str(f"elapsed={self._fmt_elapsed(elapsed)}")
        self.pbar.refresh()
