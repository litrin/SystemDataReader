from base import RawDataFileReader


class SPECjbb2005Score(RawDataFileReader):

    def __init__(self, filename):
        self.filename = filename

    @property
    def throughput(self):
        content = self.grep("throughput")
        return float(content[0].split()[2])
