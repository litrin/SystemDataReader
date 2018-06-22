from .base import RawDataFileReader
import pandas as pd
import re


class PQoSReader(RawDataFileReader):
    headers = ['CORE', 'IPC', 'MISSES', 'LLC', 'MBL', 'MBR']
    sample_range = None

    def __init__(self, input, sample_range=None):
        self.filename = input
        self.sample_range = sample_range

    @property
    def data(self):
        data = []
        skip = 0
        for lines, entry in enumerate(self.reader()):
            if re.match(r"^\s?\d+", entry) is None:
                continue
            try:
                entry = dict(zip(self.headers, entry.split()))

                entry["IPC"] = float(entry["IPC"])
                entry["MISSES"] = int(entry["MISSES"][:-1]) << 10

                for key in ['LLC', 'MBL', 'MBR']:
                    entry[key] = float(entry[key])
                data.append(entry)
            except:
                skip += 1
                print("%s skip" % self.filename)

        df = pd.DataFrame(data)
        if self.sample_range is not None:
            df = df[self.sample_range[0]:self.sample_range[1]]

        return df

    @property
    def ipc(self):
        return self.data.groupby(["CORE"]).IPC

    @property
    def llc(self):
        return self.data.LLC

    @property
    def memory_bandwidth_total(self):
        data = self.data.groupby(["CORE"]).mean().sum()
        return data.MBL + data.MBR

    def compare_by_coresets(self, col_name):
        data = self.data[col_name]

        all_coresets = data.groupby("CORE").nunique()["CORE"].index
        result = {
            coreset: data[self.data["CORE"] == coreset].values
            for coreset in all_coresets
        }

        return pd.DataFrame(result)
