import pandas

from DataReader.base import RawDataFileReader


class MKLLinpackSummary(RawDataFileReader):
    def __init__(self, filename):
        self.filename = filename

        contents = self.reader()
        for n, row in enumerate(contents):
            if row.startswith("Performance Summary (GFlops)"):
                break

        header = contents[n + 2].split()
        contents = [i.split() for i in contents[n + 3: n + 18]]

        self.data = pandas.DataFrame(contents, columns=header, dtype="float16")

    @property
    def max(self):
        result = self.data[self.data["Maximal"] == self.data["Maximal"].max()]
        return result

