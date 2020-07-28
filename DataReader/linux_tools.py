from DataReader.base import LinuxColumnStyleOutputReader
from DataReader.helper import CPUCoreList

__all__ = ["VmstatReader", "SarReader",
           "TurbostatReader"]


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
        return data().T


class SarReader(LinuxColumnStyleOutputReader):
    """
    Please collect sar data by this command:  sar -P ALL <interval> <count>
    """
    header = ["CPU#", "user", "nice", "sys", "io", "steal", "idle"]
    data_row_regex = r"^(\d{2}:)\d{2}.*(A|P)M.*(\d+|ALL)"

    def data_formatter(self, row):
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


class TurbostatReader(LinuxColumnStyleOutputReader):
    __version__ = "18.0"
    data_row_regex = r"^(\d{1,3}|\-).*\d$"

    def set_column_name(self, column_name_list=None):
        if column_name_list is not None:
            self.header = column_name_list
        else:
            with open(self.filename) as fd:
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


if __name__ == "__main__":
    c = IOstatReader(
        r"\\shwdewajod1018\EDP4\litrin\Huawei\20200728\result-2020-07-27-193540\diskstat.txt")
    print(c.aggregate("mean").to_excel(r"\\shwdewajod1018\EDP4\litrin\Huawei\20200728\result-2020-07-27-193540\avgio.xlsx"))
    print(c.aggregate("max").to_excel(r"\\shwdewajod1018\EDP4\litrin\Huawei\20200728\result-2020-07-27-193540\maxio.xlsx"))
