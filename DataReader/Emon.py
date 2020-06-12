import os
import xml.etree.cElementTree as ET
from abc import ABCMeta

import pandas as pd

from .base import DataReaderError

__all__ = ["EMONSummaryData", "EMONDetailData", "EMONMetricFormulaReader",
           "TopDownHelper"]

# here is the version number from EDP
__ver__ = "3.9"


class EMONReaderError(DataReaderError):
    pass


class EMONDataError(DataReaderError):
    pass


class EMONReader(object):
    """
    Base Emon/edp csv data reader
    """
    __metaclass__ = ABCMeta
    # define the view workload_type
    SYSTEM = "system"
    CORE = "core"
    SOCKET = "socket"
    THREAD = "thread"

    archived_file = "emon_data.zip"

    csv_files_path = None
    _csv_file_filename_format = "%s.csv"

    excel_file_name = None
    _excel_sheet_name_format = "%s %s"

    metric_list = None

    def __init__(self, path):
        """
        :param path: folder which contains emon/edp csv files or excel file
        """
        if not os.path.exists(path):
            raise EMONReaderError("Path is not exist: %s" % path)

        if os.path.isdir(path):
            self.csv_files_path = path
        else:
            self.excel_file_name = path

    def get_file_content(self, view, check_processing_state=False):
        if self.csv_files_path is not None:
            filename = self._csv_file_filename_format % view
            abs_filename = os.path.join(self.csv_files_path, filename)

            if not os.path.exists(abs_filename):
                raise EMONReaderError(
                    "View file is not exist: %s" % abs_filename)

            df = self.read_csv(abs_filename)

        if self.excel_file_name is not None:
            sheet_name = self._excel_sheet_name_format % view
            df = self.read_excel(self.excel_file_name, sheet_name)

        return self.filter(df)

    def select_metric(self, metric):
        self.metric_list.append(metric)

    def filter(self, data_frame):
        return data_frame

    @staticmethod
    def read_csv(abs_filename):
        data = pd.read_csv(abs_filename, index_col=0, verbose=True,
                           na_filter=False, engine="python", sep=",")
        for col in data.columns:
            if col == "timestamp":
                continue
            data[col] = pd.to_numeric(data[col], errors="coerce")

        return data

    @staticmethod
    def read_excel(abs_filename, sheet_name):
        data = pd.read_excel(abs_filename, sheet_name=sheet_name,
                             index_col=0, na_filter=False, )
        for col in data.columns:
            if col == "timestamp":
                continue
            data[col] = pd.to_numeric(data[col], errors="coerce")

        return data

    @property
    def raw_data_file(self):
        if self.csv_files_path is not None:
            filename = os.path.join(self.csv_files_path, self.archived_file)
        elif self.excel_file_name is not None:
            filename = self.excel_file_name
        else:
            return None

        if os.path.exists(filename):
            return filename
        return None

    @property
    def create_time(self):
        if self.csv_files_path is not None:
            path = self.csv_files_path
        elif self.excel_file_name is not None:
            path = self.excel_file_name
        else:
            return None

        return os.path.getctime(path)

    @property
    def socket_view(self):
        return self.get_file_content(self.SOCKET)

    @property
    def system_view(self):
        return self.get_file_content(self.SYSTEM)

    @property
    def core_view(self):
        return self.get_file_content(self.CORE)

    @property
    def thread_view(self):
        return self.get_file_content(self.THREAD)


class EMONCSVReader(EMONReader):

    def __init__(self, path):
        """
        :param path: folder which contains emon/edp csv files
        """
        if not os.path.exists(path):
            raise EMONReaderError("CSV files are not exist at: %s" % path)
        self.csv_files_path = path


class EMONExcelFileReader(EMONReader):

    def __init__(self, path):
        """
        :param path: folder which contains emon/edp excel file
        """
        if not os.path.exists(path):
            raise EMONReaderError("Excel file %s is not exist!" % path)

        self.excel_file_name = path


class EMONSummaryData(EMONReader):
    _csv_file_filename_format = "__edp_%s_view_summary.csv"
    _excel_sheet_name_format = "%s view"

    def filter(self, data_frame):
        if self.metric_list is not None:
            data_frame = data_frame.loc[self.metric_list]
        return data_frame


class EMONDetailData(EMONReader):
    _csv_file_filename_format = "__edp_%s_view_details.csv"
    _excel_sheet_name_format = "details %s view"

    convert_ts = False
    fill_empty_entries = "ffill"

    def filter(self, data_frame):
        if self.convert_ts:
            data_frame["timestamp"] = pd.to_datetime(
                pd.Series(data_frame["timestamp"]))

        if self.metric_list is not None:
            data_frame = data_frame[self.metric_list]

        if self.fill_empty_entries:
            data_frame = data_frame.fillna(method=self.fill_empty_entries)

        return data_frame


class TopDownHelper(object):
    data = None

    def __init__(self, dataframe, prefix="metric_tmam"):
        """
        :param dataframe: EMONSummaryView df
        :param prefix: str define TMAM metrics's prefix or name convention
        """
        # mix cases
        prefix = prefix.lower()

        name_map = {k: k.lower() for k in dataframe.index}
        self.data = dataframe.rename(name_map, axis="index")

        # filter out top-down related metrics
        keys = filter(lambda a: a.startswith(prefix), self.data.index)
        self.data = self.filter(keys)

    def filter(self, index_list):
        """
        Filter metrics from list

        :param index_list: dict | list, when use dict, filter out + rename
        :return: df
        """
        if isinstance(index_list, dict):
            index_list = {k.lower(): v for k, v in index_list.items()}
            data = self.data[self.data.index.isin(index_list.keys())]
            data = data.rename(index_list, axis='index')
        else:
            index_list = [i.lower() for i in index_list]
            data = self.data[self.data.index.isin(index_list)]

        return data

    def get_child(self, metric_name, name_to_level=lambda a: a.count(".")):
        """
        get all childs from parent name

        :param metric_name: str, parent name
        :param name_to_level: executable, function what may convert to level
        :return: df
        """
        metric_name = metric_name.lower()

        start, end, level = -1, -1, -1
        for offset, value in enumerate(self.data.index):
            if value == metric_name:
                start = offset + 1
                level = name_to_level(value)
                continue

            if start != -1 and level == name_to_level(value):
                end = offset - 1
                break

        if start == -1:
            return None

        return self.data[start:end]


class EMONMetricFormula:
    Body = None

    def __init__(self, metric):
        self.Body = metric

    @property
    def name(self):
        return self.Body.attrib["name"]

    @property
    def formula(self):
        for element in self.Body:
            if element.tag == "formula":
                return element.text

    def get_required_event(self):
        event = []
        for element in self.Body:
            if element.tag == "event":
                event.append(element.text)
        return event

    def get_required_constant(self):
        event = []
        for element in self.Body:
            if element.tag == "constant":
                event.append(element.text)
        return event

    def get_formula_body(self):

        convert = {}
        for element in self.Body:
            if element.tag in ("constant", "event"):
                convert[element.attrib['alias']] = element.text
            else:
                continue

        formula = self.formula
        for char in convert.keys():
            formula = formula.replace(char, "{{%s}}" % char)

        for char in convert.keys():
            formula = formula.replace("{{%s}}" % char, convert[char])

        return formula

    def __str__(self):
        return "%s = %s" % (self.name, self.get_formula_body())


class EMONMetricFormulaReader:
    filename = None

    def __init__(self, filename):
        self.filename = filename

    def __iter__(self):
        file_content = ET.ElementTree(file=self.filename)
        for metric in file_content.getroot():
            yield EMONMetricFormula(metric)


EDPFormulas = EMONMetricFormulaReader
EDPFormula = EMONMetricFormula
