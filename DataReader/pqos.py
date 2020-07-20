import re

import pandas as pd

from .base import RawDataFileReader, DataCacheObject


class PQoSCSVReader:

    def __init__(self, filename):
        self.filename = filename
        self.data = pd.read_csv(filename)

    @property
    def grouped_data(self):
        return self.data.groupby("Core")

    @property
    def core_groups(self):
        return list(self.data["Core"].drop_duplicates())

    def core_set(self, core):
        if core in self.core_groups:
            return self.data[self.data["Core"] == core]
        return None

    def to_excel(self, filename):
        writer = pd.ExcelWriter(filename)
        for group in self.core_groups:
            label = group
            df = self.core_set(group)
            df.to_excel(writer, sheet_name=label)

        writer.close()


class PQoSReader(RawDataFileReader, DataCacheObject):
    headers = ['CORE', 'IPC', 'MISSES', 'LLC', 'MBL', 'MBR']
    sample_range = None

    def __init__(self, input, sample_range=None):
        self.filename = input
        self.sample_range = sample_range

    def get_content(self):
        data = []
        skip = 0
        for lines, entry in enumerate(self.reader()):
            if re.match(r"^\s*\d+", entry) is None:
                continue
            try:
                entry = dict(zip(self.headers, entry.split()))

                entry["IPC"] = float(entry["IPC"])
                # sometime there haven't breaks between column IPC and MISSES
                assert (entry["IPC"] < 4)  # resonable IPC value

                entry["MISSES"] = int(entry["MISSES"][:-1]) << 10

                for key in ['LLC', 'MBL', 'MBR']:
                    entry[key] = float(entry[key])
                data.append(entry)

            except AssertionError:
                print("spliting fail at %s, skip this record" % self.filename)

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
