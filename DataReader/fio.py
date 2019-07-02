import re

from DataReader.base import RawDataFileReader

__version__ = "fio-3.1"


class FIOReader(RawDataFileReader):
    bandwidth_reg = r"bw=(\d+.\.*\d*)([MKG])"
    filename = None

    def __init__(self, filename):
        self.filename = filename

    def get_bandwidth(self, grep_filter):
        context = self.grep(grep_filter.upper())
        value = 0
        for row in context:
            match = re.search(self.bandwidth_reg, row)
            if match is not None:
                value = float(match.group(1))
                unit = match.group(2)
                if unit == "K":
                    value = value / 10E3

                if unit == "G":
                    value = value * 10E3

        return value

    @property
    def read_bandwidth(self):
        return self.get_bandwidth("READ")

    @property
    def write_bandwidth(self):
        return self.get_bandwidth("WRITE")
