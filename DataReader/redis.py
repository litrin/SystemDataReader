import json

import pandas as pd

from .base import RawDataFileReader, DataReaderError, DataCacheObject


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
        Set redis transaction workload_type

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
            raise DataReaderError("Cannot find %s data" % trans)

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


class MemtierJsonReader:
    body = {}

    def __init__(self, filename):
        with open(filename, "r") as fp:
            self.body = json.load(fp)

    @property
    def state(self):
        return self.body["ALL STATS"]

    @property
    def total(self):
        return self.body["ALL STATS"]["Totals"]

    @property
    def latency(self):
        return self.total["Latency"]

    @property
    def ops(self):
        return self.total["Ops/sec"]

    @property
    def qps(self):
        return self.total["Ops/sec"]


class MemtierOutputReader(RawDataFileReader, DataCacheObject):
    def __init__(self, file_name):
        self.filename = file_name

    def get_content(self):
        tmp = []
        for line in self.grep_iterator(r"^\[.*latency$"):
            line = line.split()
            ops = int(line[9])
            latency = float(line[16])
            tmp.append({"ops": ops, "latency": latency})
        return pd.DataFrame(tmp)

    @property
    def latency(self):
        return self.data["latency"]

    @property
    def ops(self):
        return self.data["ops"]

    @property
    def qps(self):
        return self.data["ops"]
