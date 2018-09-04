from .base import RawDataFileReader
import pandas as pd


class PtumonSKX(RawDataFileReader):
    __version__ = 1.4

    column_name = ["Time", "Dev", "Cor", "Thr", "CFreq", "UFreq", "T", "Util",
                   "IPC", "DTS", "Temp", "Volt", "Power", "TotPwr", "TDP",
                   "TStat", "TLog", "#TL", "TMargin"]

    def __init__(self, filename):
        self.filename = filename

    def get_data(self, keyword):
        reg = r"^\d{6}\.\d{3}_\d+\,%s" % keyword.upper()
        data_entries = self.egrep(reg)
        csv_body = []
        for i in data_entries:
            row = i.split(",")
            row[0] = float(row[0][:10])
            csv_body.append(dict(zip(self.column_name, row)))

        data = pd.DataFrame(csv_body)
        return data.apply(pd.to_numeric, errors='ignore')

    def __getattr__(self, item):
        return self.get_data(item)
