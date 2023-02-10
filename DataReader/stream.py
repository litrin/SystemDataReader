class StreamSubResult(dict):
    _column_name = ["function", "bandwidth", "runtime", "min", "max"]

    def __init__(self, row):
        row = row.split()
        row[1:] = [float(i) for i in row[1:]]
        row[0] = row[0][:-1].lower()

        for k, v in zip(self._column_name, row):
            self[k] = v

    def __getattr__(self, item):
        if item in self._column_name:
            return self[item]
        return None


class StreamOutput:

    def __init__(self, filename):
        self.filename = filename

    def read_data(self):
        with open(self.filename) as fd:
            row = fd.readline()
            while row:
                if row.startswith("Copy:"):
                    self.copy = StreamSubResult(row)
                if row.startswith("Scale:"):
                    self.scale = StreamSubResult(row)
                if row.startswith("Add:"):
                    self.add = StreamSubResult(row)
                if row.startswith("Triad:"):
                    self.triad = StreamSubResult(row)
                    break

                row = fd.readline()
        fd.close()

    def __iter__(self):
        with open(self.filename) as fd:
            row = fd.readline()
            while row:
                if row.startswith("Copy:"):
                    self.copy = StreamSubResult(row)
                if row.startswith("Scale:"):
                    self.scale = StreamSubResult(row)
                if row.startswith("Add:"):
                    self.add = StreamSubResult(row)
                if row.startswith("Triad:"):
                    self.triad = StreamSubResult(row)
                    yield self

                row = fd.readline()
