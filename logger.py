

import sys


class Logger:
    def __init__(self, filename: str):
        self.console = sys.stdout
        self.file = open(filename, 'w')

    def write(self, message: str):
        self.console.write(message)
        self.file.write(message)

    def flush(self):
        self.console.flush()
        self.file.flush()
