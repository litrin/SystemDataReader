import os
import pandas as pd
import re

__all__ = ["SPECjbb2015Score", "SPECjbb2015TotalPurchaseData"]

class SPECjbb2015Score:
    filename = None

    def __init__(self, path):
        self.filename = self.get_rt_overall_file(path)

    def get_rt_overall_file(self, path):
        for folder in os.listdir(path):
            if folder.find("overall-throughput-rt.txt") != -1:
                return os.path.join(path, folder)

            elif folder.find("specjbb2015-") != -1 or folder.find(
                    "report-") != -1 or folder == "data" or folder == "rt-curve":

                result_path = os.path.join(path, folder)
                return self.get_rt_overall_file(result_path)

    @property
    def csv_content(self):
        return pd.read_csv(self.filename, sep=";", skiprows=2)

    @property
    def mean_median(self):
        return self.csv_content["median"].mean()

    @property
    def median(self):
        return self.csv_content["median"]

    @property
    def mean_95th(self):
        return self.csv_content["95-th percentile"].mean()

    def __len__(self):
        return len(self.csv_content)


class SPECjbb2015TotalPurchaseData:
    def __init__(self, filename):
        self.filename = filename

    def read_output_content(self):
        reg = re.compile(r"^\s+?TotalPurchase,")

        with open(self.filename, "r") as fd:
            while True:
                row = fd.readline()
                if len(row) is 0:
                    break
                if not reg.match(row):
                    continue
                else:
                    yield row.split(",")

    @property
    def all(self):
        data = pd.DataFrame([i[1:-1] for i in self.read_output_content()],
                            dtype=float, columns=["Success", "Partial",
                                                  "Failed", "SkipFail",
                                                  "Probes", "Samples",
                                                  "min", "p50", "p90",
                                                  "p95", "p99", "max"])
        return data

    @property
    def p50(self):
        return self.all.p50

    def __len__(self):
        return len(self.all)
