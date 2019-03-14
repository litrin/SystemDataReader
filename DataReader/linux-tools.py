from .base import RawDataFileReader
import pandas as pd


class VmstatReader(RawDataFileReader):
    # need more detail column name
    header = ["r", "b", "swpd", "free", "buff", "cache", "si", "so", "bi",
              "bo", "in", "cs", "us", "sy", "id", "wa", "st"
              ]

    def __init__(self, filename, header=None):
        self.filename = filename

        if header is not None:
            self.header = header

    def get_data_frame(self):
        data = []
        for row in self.grep_iterator(r"^\d"):
            data.append(row.split())
        df = pd.DataFrame(data, columns=self.header, dtype=float)

        return df

    @property
    def all(self):
        return self.get_data_frame()
