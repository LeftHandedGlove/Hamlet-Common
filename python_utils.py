import sys
import os


# Used to print messages to journalctl
def print_msg(msg, end=None):
    if end is None:
        print(msg)
    else:
        print(msg, end=end)
    sys.stdout.flush()
