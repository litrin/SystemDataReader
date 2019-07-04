import os

import pandas as pd

from .base import RawDataFileReader, DataCacheObject

__all__ = ["SPECjbb2015Score", "SPECjbb2015TotalPurchaseData",
           "SPECjbb2005Score"]


class SPECjbb2005Score(RawDataFileReader):

    def __init__(self, filename):
        self.filename = filename

    @property
    def throughput(self):
        content = self.grep("throughput")
        return float(content[0].split()[2])


class SPECjbb2015ResultsErr(BaseException):
    pass


class SPECjbb2015Score(object):
    """
    Read response times from specjbb2015 result path
    """
    filename = None

    def __init__(self, path):
        """
        :param path: folder for SPECjbb2015 results
        """
        self.filename = self.get_rt_overall_file(path)

    def get_rt_overall_file(self, path):
        for folder in os.listdir(path):
            if folder.find("overall-throughput-rt.txt") != -1:
                return os.path.join(path, folder)

            elif folder.find("specjbb2015-") != -1 or folder.find(
                    "report-") != -1 \
                    or folder == "data" or folder == "rt-curve":

                # recursion for file seeking
                result_path = os.path.join(path, folder)
                return self.get_rt_overall_file(result_path)

        raise SPECjbb2015ResultsErr(
            "path %s hasn't contained SPECjbb2015 results" % path)

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


class SPECjbb2015TotalPurchaseData(RawDataFileReader, DataCacheObject):
    """
    Read response times from specjbb2015 output file
    """

    # the column name in log files
    header = ["Success", "Partial", "Failed", "SkipFail", "Probes", "Samples",
              "min", "p50", "p90", "p95", "p99", "max"]

    def __init__(self, filename):
        """
        :param filename: output file name
        """
        self.filename = filename

    def read_output_content(self):
        result = []
        for row in self.grep_iterator(r"^\s+?TotalPurchase,"):
            element = row.split(",")
            result.append(element[1:-1])

        return result

    def get_content(self):
        data = pd.DataFrame(self.read_output_content(), dtype=float,
                            columns=self.header)
        return data

    @property
    def all(self):
        return self.data

    @property
    def p50(self):
        return self.all.p50

    @property
    def max(self):
        return self.all.max

    def __len__(self):
        return len(self.all)
