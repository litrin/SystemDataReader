import pandas as pd

from .base import DataCacheObject


class BasePMCCSVReader(DataCacheObject):
    filename = None
    separator = ";"

    def __init__(self, filename, default_sep=None):
        """
        Read csv file from pcm tools.

        :param filename: str
        :param default_sep: str
        """
        self.filename = filename
        if default_sep is not None:
            self.separator = default_sep

    def get_data_frame(self):
        return pd.read_csv(self.filename, sep=self.separator, index_col=False,
                           names=self.headers, skiprows=2)

    @property
    def headers(self):
        """
        PCM has 2 level column.
        This function/parameter is helper to covert them to single column.

        :return: list
        """
        fd = open(self.filename, "r")
        file_head = fd.readlines()
        fd.close()

        zip_heads = zip(file_head[0].split(self.separator),
                        file_head[1].split(self.separator))

        metric_names = []
        category = ""
        for _category, metric in zip_heads:
            # fill empty category names
            if len(_category) is not 0:
                category = _category

            metric_names.append("%s.%s" % (category, metric))

        return metric_names[:-1]


class PCMCSVReader(BasePMCCSVReader):
    """
    Ready for outputs from pcm.x
    """
    pass


class PCMMemoryCSVReader(BasePMCCSVReader):
    """
    Ready for outputs from pcm-memory.x
    """
    pass
