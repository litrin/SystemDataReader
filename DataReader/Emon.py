import os
from abc import ABCMeta
from .base import DataReaderError
import xml.etree.cElementTree as ET

import pandas as pd

__all__ = ["EMONSummaryData", "EMONDetailData", "EMONMetricFormulaReader",
           "TopDownHelper"]

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
    # define the view workload_type
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
