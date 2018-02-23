from base import RawDataFileReader


class CommandlscpuInfo(RawDataFileReader):
    __version__ = 2.23

    def __init__(self, filename=None):
        self.filename = filename

    def get_info(self, key):
        result = self.grep(key).pop(0).split(":")
        return result[1].strip()

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

    def is_support(self, feature):
        return feature.lower() in self.flags
