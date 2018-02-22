from base import RawDataFileReader


class CommandlscpuInfo(RawDataFileReader):
    __version__ = 2.23

    def __init__(self, filename):
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
    def core_list(self):
        return self.get_info("On-line CPU(s) list:")

    @property
    def numa(self):
        nodes = int(self.get_info("NUMA node(s):"))
        topo = []
        for i in range(nodes):
            key = "NUMA node%s CPU(s):" % i
            topo.append(self.get_info(key))

        return topo

    @property
    def llc(self):
        return self.get_info("L3 cache:")

    @property
    def frequency(self):
        return float(self.get_info("CPU MHz:"))

    @property
    def flags(self):
        return self.get_info("Flags:").split()

    def is_support(self, feature):
        return feature.lower() in self.flags
