from base import RawDataFileReader
import pandas as pd
import re


class PQoSReader(RawDataFileReader):
    headers = ['CORE', 'IPC', 'MISSES', 'LLC', 'MBL', 'MBR']

    def __init__(self, input):
        self.filename = input

    @property
    def data(self):
        data = []
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
                continue

        return pd.DataFrame(data)

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
