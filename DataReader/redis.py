class RedisBenchmarkData(object):
    content = []
    trans_data = []

    def __init__(self, filename):
        with open(filename, "r") as fd:
            self.content = fd.readlines()

    def set_transaction(self, trans):
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

