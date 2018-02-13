import pandas as pd
import os

__all__ = ["EMONSummaryData", "EMONDetailData"]

# here is the version number from EDP
__ver__ = "3.6"


class EMONReaderError(BaseException):
    pass


class EMONCSVReader(object):
    """
    Base Emon/edp csv data reader
    """

    # define the view type
    SYSTEM = "system"
    CORE = "core"
    SOCKET = "socket"
    # THREAD = "thread"

    path = "."

    _filename_format = "%s.csv"

    def __init__(self, path):
        """
        :param path: folder which contains emon/edp csv files
        """
        self.path = path

    def get_file_content(self, view):
        filename = self._filename_format % view

        abs_filename = os.path.join(self.path, filename)
        if not os.path.exists(abs_filename):
            raise EMONReaderError("file not exist: %s" % abs_filename)

        if not os.path.exists(os.path.join(self.path, "emon_data.zip")):
            raise EMONReaderError(
                "Process does not finish at %s" % abs_filename)

        return self.read_csv(abs_filename)

    def read_csv(self, abs_filename):
        data = pd.read_csv(abs_filename, index_col=0, low_memory=False,
                           na_filter=False)

        data = data.convert_objects(convert_numeric=True)

        return data


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
