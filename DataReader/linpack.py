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




if __name__ == "__main__":
    c = MKLLinpackSummary(
        r"\\shwdewajod1018\EDP4\meiqi\D1\linpack\serial\0x1-24-25,72-73\linpack.txt")
    print(c.data)
