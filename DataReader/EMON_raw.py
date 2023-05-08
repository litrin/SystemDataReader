import pandas


class EMONEvent(list):
    """
    Vector of sample
    """
    name = "UNKNOWN"
    ts = 0

    def __init__(self, row: str):
        row = row.replace(",", "").split()
        self.name = row[0]
        self.ts = int(row[1])

        for i in row[2:]:
            self.append(int(i))

    def __str__(self):
        return "%s: %s: [%s]" % (self.name, self.ts, " ".join(map(str, self)))


class EMONRawFile:
    """
    Read EMON raw data from emon -i output file.
    """
    def __init__(self, filename):
        self.filename = filename

    def __getitem__(self, event_name):
        """
        return an iterator for dedicate event
        """
        self.event = event_name
        fd = open(self.filename)
        while True:
            row = str(fd.readline())
            if row.startswith(self.event):
                yield EMONEvent(row)
            if row is None or len(row) == 0:
                break
        fd.close()

    def to_dataframe(self, event_name: str) -> pandas.DataFrame:
        """
        Return a dataframe of event
        """
        return pandas.DataFrame(self[event_name])
