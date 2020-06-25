import sys
import os


# Used to print messages to journalctl
def print_msg(msg, end=None):
    if end is None:
        print(msg)
    else:
        print(msg, end=end)
    sys.stdout.flush()


# Get the top level Hamlet directory
if getattr(sys, 'frozen', False):
    hamlet_top_dir = os.path.dirname(sys.executable)
else:
    hamlet_top_dir = os.path.dirname(os.path.realpath(__file__))