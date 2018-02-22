import os
import pandas as pd


class CSVCombineHelper(object):
    builder = None
    file_list = []
    data_set = None

    def __init__(self, path, builder=None):
        self.builder = builder
        for f in os.listdir(path):
            if f.find(".") != -1:
                continue
            self.file_list.append(os.path.join(path, f))

    def build_data_object(self, path):
        return self.builder(path)

    def select_data(self, data_entry):
        return data_entry

    def to_excel(self, filename="data.xls"):
        result = {}
        for path in self.file_list:
            reader = self.build_data_object(path)
            data = self.select_data(reader)

            label = os.path.split(path)[-1]
            result[label] = data

        result = pd.DataFrame(result)
        result.to_excel(filename)
