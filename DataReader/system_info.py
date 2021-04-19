import pandas

from DataReader.base import RawDataFileReader


class BaseCLIOptionReader(RawDataFileReader):
    def __init__(self, filename=None):
        self.filename = filename

    def get_info(self, key):
        result = self.grep(key).pop(0).split(":")
        return result[1].strip()


class CLIOptionReader(BaseCLIOptionReader):
    def __getattr__(self, item):
        return self.get_info(item)


class CommandlscpuInfo(BaseCLIOptionReader):
    __version__ = 2.23

    @property
    def core_number(self):
        return int(self.get_info("CPU(s):"))

    @property
    def model(self):
        return self.get_info("Model name")

    @property
    def socket(self):
        return int(self.get_info("Socket(s)"))

    @property
    def core_pre_socket(self):
        return int(self.get_info("Core(s) per socket:"))

    @property
    def core_list(self):
        return self.get_info("On-line CPU(s) list:")

    @property
    def numa(self):
        nodes = int(self.get_info("NUMA node(s):"))
        return [self.get_info("NUMA node%s CPU(s):" % i) for i in range(nodes)]

    @property
    def llc(self):
        return self.get_info("L3 cache:")

    @property
    def frequency(self):
        return float(self.get_info("CPU MHz:"))

    @property
    def flags(self):
        return self.get_info("Flags:").split()

    @property
    def ht_enabled(self):
        return "1" != self.get_info("Thread(s) per core")

    @property
    def cat_l3_pre_bitway(self):
        if not self.is_support("cat_l3"):
            return 0

        if "1024K" == self.get_info("L2 cache:"):
            total_bitways = 11
        else:
            total_bitways = 20

        llc_capacity = int(self.llc[:-1])
        return llc_capacity / total_bitways

    @property
    def family_stepping(self):
        # stepping 4 is skx, 5 is clx
        return (int(self.get_info("CPU family")),
                int(self.get_info("Stepping")))

    def is_support(self, feature):
        return feature.lower() in self.flags


class ProcInterrupt(RawDataFileReader):
    """
    Summary system level interrups by /proc/interrups

    """
    def __init__(self, filename="/proc/interrupts"):
        self.filename = filename
        self._get_data()

    def _get_data(self):
        data = self.reader()
        header = ["INT"]
        for i in data[0].split():
            header.append(i)
        for i in ("dev", "mode", "function"):
            header.append(i)

        tmp = []
        for row in data[1:]:
            row = row.split()
            row[0] = row[0][:-1]
            for i, v in enumerate(row):
                try:
                    row[i] = int(v)
                except ValueError:
                    row[i] = v

            tmp.append(dict(zip(header, row)))

        self.data = pandas.DataFrame(tmp)
        self.data.index = self.data.INT.values
        self.data = self.data.drop(columns="INT")

    @property
    def devices(self):
        """
        All devices

        :return:
        """
        return self.data.dev.unique()

    @property
    def interrupts(self):
        """
        CPU based interrupts summary

        :return:
        """

        return self.data[self.data.columns[
            self.data.columns.map(lambda a: a.startswith("CPU"))]].dropna()

    def filter_device(self, dev):
        """
        Filter out by device name

        :param dev:
        :return:
        """
        if dev in self.devices:
            return self.data.where(self.data.dev == dev).dropna()
        return None

    def filter_function(self, handle):
        """
        Filter out by function name prefix

        :param handle:
        :return:
        """
        arr = [str(i).startswith(handle) for i in self.data.function.values]
        return self.data.iloc[arr]

    def summary(self, data=None):
        """
        CPU grouped interruption summary

        :param data: DF | filtered data
        :return: list
        """
        if data is None:
            data = self.interrupts

        result = []
        for c in data.columns:
            result.append(data[c].sum())

        return result
