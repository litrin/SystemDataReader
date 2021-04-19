from abc import ABCMeta

import pandas as pd

from DataReader.base import LinuxColumnStyleOutputReader
from DataReader.helper import CPUCoreList

__all__ = ["VmstatReader", "SarCPUstateReader", "SarNetworkstateReader",
           "IOstatReader", "TurbostatReader"]


class VmstatReader(LinuxColumnStyleOutputReader):
    data_row_regex = r"^\s?\d"
    # need more detail column name
    header = ["r", "b", "swpd", "free", "buff", "cache", "si", "so", "bi",
              "bo", "in", "cs", "us", "sy", "id", "wa", "st"]


class IOstatReader(LinuxColumnStyleOutputReader):
    data_row_regex = r"^[a-z]"
    header = ['Device:', 'rrqm/s', 'wrqm/s', 'r/s', 'w/s', 'rkB/s', 'wkB/s',
              'avgrq-sz', 'avgqu-sz', 'await', 'r_await', 'w_await', 'svctm',
              '%util']

    def get_device(self, dev):
        return self.row_filter("Device:", dev)

    def aggregate(self, summary="mean"):
        data = getattr(self.data.groupby("Device:"), summary)
        return data()


class BaseSarReader(LinuxColumnStyleOutputReader):
    __metaclass__ = ABCMeta

    header = ["Time", "AMPM"]
    data_row_regex = r"^(\d{2}:)\d{2}.*(A|P)M.*"

    # data_category = ""

    @property
    def data_category(self):
        return self.header[2]

    def get_content(self):
        df = super().get_content()
        df["Time"] = df["Time"] + " " + df["AMPM"]
        del (df["AMPM"])

        df["Time"] = pd.DatetimeIndex(df["Time"])  # covert to TS data
        return df

    def aggregate(self, summary="mean"):
        method = getattr(self.data.groupby(self.data_category), summary)
        return method()

    def __getitem__(self, item):
        return self.data[self.data[self.data_category] == item]

    def to_excel(self, filename):
        writer = pd.ExcelWriter(filename)

        for group in self.distinct(self.data_category):
            label = "%s %s" % (self.data_category, group)
            df = self[group]
            del (df[self.data_category])
            df.to_excel(writer, sheet_name=label, index=False)

        writer.close()


class SarCPUstateReader(BaseSarReader):
    """
    Please collect sar data by this command:  sar -P ALL <interval> <count>
    """
    header = ["Time", "AMPM", "CPU#", "user", "nice", "sys", "io", "steal",
              "idle"]
    data_row_regex = r"^(\d{2}:)\d{2}.*(A|P)M.*(\d+|ALL)"


class SarNetworkstateReader(BaseSarReader):
    """
    Please collect sar data by this command:  sar -n DEV <interval> <count>
    """
    header = ["Time", "AMPM", 'IFACE', 'rxpck/s', 'txpck/s', 'rxkB/s',
              'txkB/s', 'rxcmp/s', 'txcmp/s', 'rxmcst/s']

    data_row_regex = r"^(\d{2}:){2}\d{2}.*(A|P)M\s+[a-z]"


class TurbostatReader(LinuxColumnStyleOutputReader):
    __version__ = "19.08"
    data_row_regex = r"^\s+?(\d{1,3}|\-).*\d$"

    def set_column_name(self, column_name_list=None):
        if column_name_list is not None:
            self.header = column_name_list
        else:
            with open(self.filename) as fd:
                column_name_list = fd.readline()
                while column_name_list.find("Avg_MHz") == -1:
                    column_name_list = fd.readline()

            self.header = column_name_list.split()

    def data_formatter(self, row):
        row = row.split()
        # mark global state as "-1"

        if row[0] == "-":
            row[0], row[1], row[2] = -1, -1, -1
        return row

    @property
    def cores(self):
        core_list = map(int, self.data["CPU"].unique())
        return CPUCoreList(core_list)

    def __getitem__(self, item):
        if item == 'all' or item == -1:
            return self.aggregate

        core_list = CPUCoreList(item)
        return self.data[self.data["CPU"].isin(core_list.get_list())]

    @property
    def aggregate(self):
        return self.data[self.data["CPU"] == -1]
