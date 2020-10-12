from abc import abstractmethod

import pandas as pd

from DataReader.base import RawDataFileReader


class BasePtuReader(RawDataFileReader):
    def __init__(self, filename):
        self.filename = filename

    def set_header(self, header):
        self.column_name = header

    @abstractmethod
    def get_data(self, keyword):
        pass

    def __getattr__(self, item):
        return self.get_data(item)

    def __getitem__(self, item):
        return self.get_data(item)


class PtumonSKX(BasePtuReader):
    """
    CLI: ./ptumon -csv > filename.csv
    """
    __version__ = 1.4
    column_name = ["Time", "Dev", "Cor", "Thr", "CFreq", "UFreq", "T", "Util",
                   "IPC", "DTS", "Temp", "Volt", "Power", "TotPwr", "TDP",
                   "TStat", "TLog", "#TL", "TMargin", "SID"]

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


class PtumonICX(BasePtuReader):
    """
    cli: ./ptu -mon -csv > filename.csv
    """
    __version__ = 2.0
    column_name = ["Index", "Device", "Cor", "Thr", "CFreq", "UFreq", "Util",
                   "IPC", "C0", "C1", "C6", "PC2", "PC6", "MC", "Ch", "Sl",
                   "Temp", "DTS", "Power", "Volt", "TStat", "TLog", "#TL",
                   "TMargin"]

    def get_data(self, keyword):
        reg = r"^\s*\d+\s*%s" % keyword.upper()
        data_entries = self.egrep(reg)

        csv_body = []
        for i in data_entries:
            row = i.split()
            row = dict(zip(self.column_name, row))
            csv_body.append(row)

        data = pd.DataFrame(csv_body)
        return data.apply(pd.to_numeric, errors='ignore')
