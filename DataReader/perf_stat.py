import pandas as pd

from .base import RawDataFileReader, DataCacheObject


class PerfStatReader(RawDataFileReader, DataCacheObject):
    """
    perf stat out put file reader
    example:

    perf stat -e cycles,instructions  -I 100 -o event.out

    todo: need to add cgroup info into dataframe
    """
    reg = r"\s+\d+\.\d+\s+(\d|\,)+"

    def __init__(self, filename):
        self.filename = filename

    def get_content(self):
        data = []
        for row in self.grep_iterator(self.reg):
            row = row.split()
            ts = float(row[0])
            value = int(row[1].replace(",", ""))
            event = row[2]

            data.append([ts, value, event])

        df = pd.DataFrame(data, columns=["ts", "value", "event"])
        return df

    @property
    def header(self):
        return self.data["event"].unique()

    def __getitem__(self, item):
        if item not in self.header:
            raise IndexError("Can not find event: '%s' in this file" % item)

        return self.data[self.data["event"] == item]
