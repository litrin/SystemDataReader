import os
from abc import ABCMeta

import pandas as pd

from .base import DataReaderError

__all__ = ["EMONSummaryData", "EMONDetailData"]

# here is the version number from EDP
__ver__ = "3.9"


class EMONReaderError(DataReaderError):
    pass


class EMONDataError(DataReaderError):
    pass


class EMONCSVReader(object):
    """
    Base Emon/edp csv data reader
    """
    __metaclass__ = ABCMeta
    # define the view type
    SYSTEM = "system"
    CORE = "core"
    SOCKET = "socket"
    # THREAD = "thread"

    archived_file = "emon_data.zip"

    path = "."

    _filename_format = "%s.csv"
    metric_list = None

    def __init__(self, path):
        """
        :param path: folder which contains emon/edp csv files
        """
        self.path = path

    def get_file_content(self, view, check_processing_state=False):
        filename = self._filename_format % view

        abs_filename = os.path.join(self.path, filename)
        if not os.path.exists(abs_filename):
            raise EMONReaderError("file not exist: %s" % abs_filename)

        if check_processing_state and not os.path.exists(
                os.path.join(self.path, "emon_data.zip")):
            raise EMONDataError(
                "Process does not finish at %s" % abs_filename)

        df = self.read_csv(abs_filename)
        if self.metric_list is not None:
            df = df[df.index.isin(self.metric_list)]

        return df

    def set_metric_list(self, metrics):
        self.metric_list = metrics

    @staticmethod
    def read_csv(abs_filename):
        data = pd.read_csv(abs_filename, index_col=0, low_memory=False,
                           na_filter=False, engine="c")
        for col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

        return data

    @property
    def raw_data_file(self):
        filename = os.path.join(self.path, self.archived_file)
        if os.path.exists(filename):
            return filename
        else:
            return None

    @property
    def create_time(self):
        return os.path.getctime(self.path)


class EMONSummaryData(EMONCSVReader):
    _filename_format = "__edp_%s_view_summary.csv"

    @property
    def system_view(self):
        return self.get_file_content(self.SYSTEM)

    @property
    def core_view(self):
        return self.get_file_content(self.CORE)

    @property
    def socket_view(self):
        return self.get_file_content(self.SOCKET)


class EMONDetailData(EMONCSVReader):
    _filename_format = "__edp_%s_view_details.csv"

    convert_ts = False

    def get_file_content(self, view):
        data = super(EMONDetailData, self).get_file_content(view)
        if self.convert_ts:
            data["timestamp"] = pd.to_datetime(pd.Series(data["timestamp"]))

        return data

    @property
    def system_view(self):
        return self.get_file_content(self.SYSTEM)

    @property
    def socket_view(self):
        return self.get_file_content(self.SOCKET)

    @property
    def core_view(self):
        return self.get_file_content(self.CORE)


class TopDownAnalyzerHelper(object):
    data = None

    def __init__(self, dataframe, prefix="metric_tmam"):
        """
        helper to select TMAM related metrics

        :param dataframe: EMONSummary Datafram
        :param prefix: str, metric prefix of TMAM metric
        """
        # case mix
        prefix = prefix.lower()
        self.data = dataframe
        self.data.index = [i.lower() for i in self.data.index]

        keys = filter(lambda a: a.startswith(prefix), self.data.index)
        # convert values to percentage
        self.data = self.filter(keys)

    def filter(self, index_list):
        """
        Filter out by index list, if needed rename index name

        :param index_list: list | dict
                select by list or dict.key(); rename by dict.value()
        :return: dataframe
        """
        if isinstance(index_list, dict):
            selector = [i.lower() for i in index_list.keys()]
            data = self.data[self.data.index.isin(selector)]
            data = data.rename(index_list, axis='index')

            return data

        selector = [i.lower for i in index_list]
        data = self.data[self.data.index.isin(selector)]

        return data
