import os
from abc import ABCMeta
from .base import DataReaderError
import xml.etree.cElementTree as ET

import pandas as pd

__all__ = ["EMONSummaryData", "EMONDetailData", "EMONMetricFormulaReader"]

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


class TopDownAnalyzer(object):
    data = None

    def __init__(self, dataframe, prefix=None):
        name_map = {k: k.lower() for k in dataframe.index}
        self.data = dataframe.rename(name_map, axis="index")

        if prefix is None:
            prefix = "metric_tmam"
        else:
            prefix = prefix.lower()

        keys = filter(lambda a: a.startswith(prefix), self.data.index)
        # convert values to percentage
        self.data = self.filter(keys)

    def filter(self, index_list):
        if isinstance(index_list, dict):
            index_list = {k.lower(): v for k, v in index_list.items()}
            data = self.data[self.data.index.isin(index_list.keys())]
            data = data.rename(index_list, axis='index')
        else:
            index_list = [i.lower() for i in index_list]
            data = self.data[self.data.index.isin(index_list)]
        return data

    @property
    def ipc(self):
        keys = {
            "metric_TMAM_Info_CoreIPC": "ipc"
        }
        return self.filter(keys) * 100

    @property
    def memory_level_parallelism(self):
        keys = {
            "metric_TMAM_Info_Memory Level Parallelism":
                "Memory Level Parallelism"
        }
        return self.filter(keys) * 100

    @property
    def tread_active(self):
        keys = {
            "metric_TMAM_Info_cycles_both_threads_active(%)":
                "Threads Active rate"
        }
        return self.filter(keys)

    @property
    def top_level(self):
        keys = {"metric_TMAM_Frontend_Bound(%)": "Frontend Bound",
                "metric_TMAM_Backend_bound(%)": "Backend Bound",
                "metric_TMAM_Bad_Speculation(%)": "Bad Speculation",
                "metric_TMAM_Retiring(%)": "Retiring"}

        result = self.filter(keys)
        return result

    @property
    def backend(self):
        keys = {"metric_TMAM_Backend_bound(%)": "Backend Bound Total",
                "metric_TMAM_..Memory_Bound(%)": "Memory Bound",
                "metric_TMAM_..Core_Bound(%)": "Core Bound"}
        return self.filter(keys)

    @property
    def backend_core(self):
        keys = {"metric_TMAM_..Core_Bound(%)": "Backend Core Bound Total",
                "metric_TMAM_....Divider(%)": "iDevider",
                "metric_TMAM_....Ports_Utilization(%)": "All port utilization",
                "metric_TMAM_......0_Ports_Utilized(%)": "Port 0 utilization",
                "metric_TMAM_......1_Port_Utilized(%)": "Port 1 utilization",
                "metric_TMAM_......2_Ports_Utilized(%)": "Port 2 utilization",
                "metric_TMAM_......3m_Ports_Utilized(%)": "Port 3m utilization"
                }

        return self.filter(keys)

    @property
    def backend_memory(self):
        keys = {
            "metric_TMAM_..Memory_Bound(%)": "Backend Memory Bound Total",
            "metric_TMAM_....L1_Bound(%)": "L1 Bound",
            "metric_TMAM_......DTLB_Load(%)": "L1 DTLB Load",
            "metric_TMAM_......Store_Fwd_Blk(%)": "L1 Store forward block",
            "metric_TMAM_......Lock_Latency(%)": "L1 Lock Latency",
            "metric_TMAM_....L2_Bound(%)": "L2 Bound",
            "metric_TMAM_....L3_Bound(%)": "L3 Bound",
            "metric_TMAM_......Contested_Accesses(%)": "L3 Contested Accesses",
            "metric_TMAM_......Data_Sharing(%)": "L3 Data Sharing",
            "metric_TMAM_......L3_Latency(%)": "L3 Latency",
            "metric_TMAM_......L3_Bandwidth(%)": "L3 Bandwidth",
            "metric_TMAM_......SQ_Full(%)": "L3 SQ Full",
            "metric_TMAM_....MEM_Bound(%)": "DRAM Bound",
            "metric_TMAM_......MEM_Bandwidth(%)": "Ext. Memory Bandwidth",
            "metric_TMAM_......MEM_Latency(%)": "Ext. Memory Latency",
            "metric_TMAM_....Stores_Bound(%)": "Stores Bound",
            "metric_TMAM_......DTLB_Store(%)": "Stores DTLB",
        }

        return self.filter(keys)

    @property
    def bad_speculaction(self):
        keys = {"metric_TMAM_Bad_Speculation(%)": "Bad Speculation Total",
                "metric_TMAM_..Branch_Mispredicts(%)": "Branch Mis-predicts",
                "metric_TMAM_..Machine_Clears(%)": "Machine clears",
                }

        return self.filter(keys)

    @property
    def frontend(self):
        keys = {"metric_TMAM_Frontend_Bound(%)": "Frontend Bound Total",
                "metric_TMAM_..Frontend_Latency(%)": "Frontend Latency",
                "metric_TMAM_....ICache_Misses(%)": "Latency iCache Misses",
                "metric_TMAM_....ITLB_Misses(%)": "Latency ITLB Misses",
                "metric_TMAM_....Branch_Resteers(%)": "Latency Branch Resteers",
                "metric_TMAM_....DSB_Switches(%)": "Latency DSB Switches",
                "metric_TMAM_....MS_Switches(%)": "Latency MS Switches",
                "metric_TMAM_..Frontend_Bandwidth(%)": "Frontend Bandwidth",
                }

        return self.filter(keys)

    @property
    def retiring(self):
        keys = {"metric_TMAM_Retiring(%)": "Retiring Total",
                "metric_TMAM_..Base(%)": "Retiring Base",
                "metric_TMAM_....FP_Arith(%)": "Base FP Arith.",
                "metric_TMAM_....Other(%)": "Base Other",
                "metric_TMAM_..Microcode_Sequencer(%)": "Microcode Sequencer",
                }

        return self.filter(keys)


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
