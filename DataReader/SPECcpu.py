import re

from DataReader.base import RawDataFileReader

__all__ = ["SPECCPU2006Score", "SPECCPU2017Score"]


class BaseSPECcpuScore(RawDataFileReader):
    score_row = []
    default_component = "mcf"
    filename = None

    def __init__(self, filename=None, component=None):
        if filename is not None:
            self.filename = filename

        if component is not None:
            self.set_component(component)

    def set_component(self, component):
        component = component.lower()
        # full component format
        regex = r"^\d{3}\.%s+.?(\s+\d+){3}" % component
        for score_content in self.grep_iterator(regex):
            break
        self.score_row = list(
            filter(lambda a: len(a) > 0, score_content.split()))

    @property
    def rate(self):
        return float(self.score_row[3])

    def __getitem__(self, item):
        self.set_component(item)
        return self.rate

    @property
    def copies(self):
        return int(self.score_row[1])

    @property
    def run_time(self):
        return int(self.score_row[2])

    @property
    def component_name(self):
        return self.score_row[0]


class SPECCPU2006Score(BaseSPECcpuScore):
    filename = "CINT2006.001.ref.txt"


class SPECCPU2017Score(BaseSPECcpuScore):
    filename = "CPU2017.001.fpspeed.refspeed.txt"


class SPECCPU2017Components:
    dictionary = {
        "500": "perlbench_r",
        "502": "gcc_r",
        "505": "mcf_r",
        "520": "omnetpp_r",
        "523": "xalancbmk_r",
        "525": "x264_r",
        "531": "deepsjeng_r",
        "541": "leela_r",
        "548": "exchange2_r",
        "557": "xz_r",

        "503": "bwaves_r",
        "507": "cactuBSSN_r",
        "519": "lbm_r",
        "521": "wrf_r",
        "527": "cam4_r",
        "528": "pop2_r",  # only for test "speed"
        "538": "imagick_r",
        "544": "nab_r",
        "549": "fotonik3d_r",
        "554": "roms_r",

    }

    _reg = re.compile(r"^([5|6]\d{2})?\.?(\S*\_?[r|s]?)?$")

    comp_id = 0
    name = "unknown"
    type = "rate"

    def __init__(self, name):
        group = self._reg.search(name).groups()

        if group[0] is not None:
            self.comp_id = int(group[0])
        elif group[1] is not None:
            self.name = group[1]
        else:
            raise NameError("%s is not a SPECcpu2017 component name")

        if self.name[-2:] == "_s" or self.comp_id > 599:
            self.type = "speed"
            self.name = self.dictionary[str(self.comp_id - 100)]
            self.name = "%ss" % (self.name[:-1])
        else:
            self.type = "rate"
            if self.comp_id == 0:
                for k, v in self.dictionary.items():
                    if v == self.name:
                        break
                self.comp_id = k
            if self.name == "unknown":
                self.name = self.dictionary[str(self.comp_id)]

    @property
    def full_name(self):
        return "%s.%s" % (self.comp_id, self.name)

    def __str__(self):
        return self.full_name
