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


class MPLinpackSummary:

    def __init__(self, filename):
        self.filename = filename
        i = 0
        with open(self.filename) as fd:
            while True:
                line = fd.readline()
                if line is None:
                    break

                if line.startswith("="):
                    i += 1
                    if i < 3:
                        continue
                    if i > 3:
                        break

                    line = fd.readline()
                    header = line.split()[1:]
                    fd.readline()
                    line = fd.readline()
                    values = map(float, line.split()[1:])

            fd.close()

        self.data = dict(zip(header, values))

        self.data["size"] = self.data["N"]
        self.data["result"] = self.data["Gflops"]

    def __getattr__(self, item):
        if item in self.data.keys():
            return self.data[item]
        return self.data
