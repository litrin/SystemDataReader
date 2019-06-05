from .base import RawDataFileReader
import pandas as pd


class PerfStatReader(RawDataFileReader):

    def __init__(self, filename):
        self.filename = filename

    def get_event(self, event_name, regex=False):
        if not regex:
            event_name = ".*%s.*" % event_name.lower()

        for row in list(self.grep_iterator(event_name)):
            row = row.split()
            value = int(row[1].replace(",", ""))
            yield value

    def __getattr__(self, item):
        return pd.Series(self.get_event(item))

    def get_dataframe(self, columns):
        data = {field: getattr(self, field) for field in columns}
        return pd.DataFrame(data)
