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
