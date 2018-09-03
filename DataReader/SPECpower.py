import pandas as pd
from .base import RawDataFileReader


class SPECpower2008(RawDataFileReader):
    row_num = (0, -1)
    column_name = ["TargetLoad", "ActualLoad", "TargetjOPS", "ActualjOPS"]

    def __init__(self, filename, row_num=(225, 235)):
        self.filename = filename
        self.row_num = row_num

    @property
    def content(self):
        data = []
        for row in self.read_line(self.row_num[0], self.row_num[1]):
            temp = []
            for i, v in enumerate(row.split("|")):
                temp.append(v.replace(" ", ""))
                if i > 1:
                    temp[i] = int(v.replace(",", ""))

            row = dict(zip(self.column_name, temp))

            data.append(row)

        return pd.DataFrame(data)
