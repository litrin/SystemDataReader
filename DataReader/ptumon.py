import pandas as pd

from .base import RawDataFileReader


class PtumonSKX(RawDataFileReader):
    __version__ = 1.4

    column_name = ["Time", "Dev", "Cor", "Thr", "CFreq", "UFreq", "T", "Util",
                   "IPC", "DTS", "Temp", "Volt", "Power", "TotPwr", "TDP",
                   "TStat", "TLog", "#TL", "TMargin", "SID"]

    def __init__(self, filename):
        self.filename = filename

    def set_header(self, header):
        self.column_name = header

    def get_data(self, keyword):
        reg = r"^\d{6}\.\d{3}_\d+\s*\,%s" % keyword.upper()
        data_entries = self.egrep(reg)
        csv_body = []
        for i in data_entries:
            row = i.split(",")
            row = dict(zip(self.column_name, row))
            row["Time"], row["SID"] = tuple(row["Time"].split("_"))

            csv_body.append(row)

        data = pd.DataFrame(csv_body)
        return data.apply(pd.to_numeric, errors='ignore')

    def __getattr__(self, item):
        return self.get_data(item)

    def __getitem__(self, item):
        return self.get_data(item)
