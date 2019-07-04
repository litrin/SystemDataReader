import pandas as pd

from .base import RawDataFileReader, DataCacheObject
from .helper import CPUCoreList

__all__ = ["VmstatReader", "SarReader"]


class VmstatReader(RawDataFileReader, DataCacheObject):
    # need more detail column name
    header = ["r", "b", "swpd", "free", "buff", "cache", "si", "so", "bi",
              "bo", "in", "cs", "us", "sy", "id", "wa", "st"
              ]

    def __init__(self, filename, header=None):
        self.filename = filename

        if header is not None:
            self.header = header

    def get_content(self):
        data = []
        for row in self.grep_iterator(r"^\s?\d"):
            data.append(row.split())
        df = pd.DataFrame(data, columns=self.header, dtype=float)

        return df

    @property
    def all(self):
        return self.data


class SarReader(RawDataFileReader, DataCacheObject):
    """
    Please collect sar data by this command:  sar -P ALL <interval> <count>
    """
    header = ["CPU#", "user", "nice", "sys", "io", "steal", "idle"]
    data_row_reg = r"^(\d{2}:)\d{2}.*(A|P)M.*(\d+|ALL)"

    def __init__(self, filename, header=None):
        self.filename = filename

    def get_content(self):
        data = []
        for row in self.grep_iterator(self.data_row_reg):
            data.append(self.format_row(row))

        return pd.DataFrame(data, columns=self.header)

    def format_row(self, row):
        row = row.split()
        data = [row[2]]  # column 2 is core ID

        for element in row[3:]:
            data.append(float(element[:-1]))

        return data

    def __getitem__(self, item):
        if item.lower() == "all":
            df = self.data
            ret = df[df['CPU#'] == 'all']
            return ret

        core_list = map(str, CPUCoreList(item))
        df = self.data
        ret = df[df["CPU#"].isin(core_list)]
        return ret
