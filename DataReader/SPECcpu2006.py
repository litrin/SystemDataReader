from base import RawDataFileReader


class SPECCPU2006Score(RawDataFileReader):
    filename = None
    default_component = "mcf"

    score_row = []

    def __init__(self, filename=r"./CINT2006.001.ref.txt", component=None):
        self.filename = filename
        if component is not None:
            self.default_component = component.lower()

    def set_component(self, component=None):
        if component is None:
            component = self.default_component
        else:
            component = component.lower()

        regex = r"^\d{3}\.%s+(\s+\d+){3}" % component

        score_content = self.egrep(regex).pop()
        self.score_row = filter(lambda a: len(a) > 0, score_content.split())

    @property
    def component_name(self):
        return self.score_row[0]

    @property
    def copies(self):
        return int(self.score_row[1])

    @property
    def run_time(self):
        return int(self.score_row[2])

    @property
    def rate(self):
        return int(self.score_row[3])
