from .base import RawDataFileReader


class RedisBenchmarkData(RawDataFileReader):
    """
    Read redis-benchmark results
    """
    content = []
    trans_data = []

    def __init__(self, filename, trans=None):
        """
        :param filename: redis-benchmark output file
        """
        self.filename = filename
        self.content = self.reader()

        if trans is not None:
            self.set_transaction(trans)

    def set_transaction(self, trans):
        """
        Set redis transaction type

        :param trans: trans code
        :return: None
        """

        # todo: here needs to add logic to validate the trans code.
        tag = "====== %s" % trans.upper()
        found_tag = False
        self.trans_data = []

        for row in self.content:
            if row.find("%") != -1 or len(row) < 3:
                continue

            if row.find(tag) != -1:
                found_tag = True
                continue

            if found_tag:
                if row.find("====== ") != -1:
                    break
                else:
                    self.trans_data.append(row)

        if not found_tag:
            raise EOFError("Cannot find %s data" % trans)

    @property
    def client(self):
        raw = self.trans_data[1].split()[0]
        return int(raw)

    @property
    def qps(self):
        raw = self.trans_data[4].split()[0]
        return float(raw)

    @property
    def total_request(self):
        raw = self.trans_data[0].split()[0]
        return int(raw)

    @property
    def total_time(self):
        raw = self.trans_data[0].split()[-2]
        return float(raw)
