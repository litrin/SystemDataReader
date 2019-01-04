import pandas as pd


class BasePMCCSVReader:
    filename = None
    sep = ";"

    def __init__(self, filename, default_sep=";"):
        self.filename = filename
        self.sep = default_sep

    def get_data_frame(self):

        return pd.read_csv(self.filename, sep=self.sep, index_col = False,
                           names=self.headers, skiprows=2)

    @property
    def headers(self):
        fd = open(self.filename, "r")
        file_head = fd.readlines(2)
        zip_heads = zip(file_head[0].split(self.sep),
                        file_head[1].split(self.sep))

        metric_names = []
        category = ""
        for _category, metric in zip_heads:
            if len(_category) is not 0:
                category = _category

            metric_names.append("%s.%s" % (category, metric))

        return metric_names[:-1]


class PCMCSVReader(BasePMCCSVReader):
    pass