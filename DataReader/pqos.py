import re
import xml.etree.cElementTree as ET

import pandas as pd
from matplotlib import pyplot as plt


class BasePQoSReader:
    filename = None
    data_indexes = ['IPC', 'LLC Misses', 'LLC[KB]', 'MBL[MB/s]', 'MBR[MB/s]']

    def __init__(self, filename, ts=False, total_bandwidth=False):
        self.filename = filename
        self.read_file()

        if ts:
            # when enabled ts, covert as time serail data
            self.data["Time"] = pd.DatetimeIndex(self.data["Time"])

        if total_bandwidth:
            # if enabled, will calculate total memory bw.
            # Strongly suggest to enable it when system NUMA mode disabled.
            self.data["Total MemoryBW"] = self.data["MBL[MB/s]"] + self.data[
                "MBL[MB/s]"]

    def read_file(self):
        pass

    def data_clean(self, column, fun):
        """

        :param column: str column name
        :param fun: runable a function to data clean
        :return: None
        """
        self.data[column] = fun(self.data[column])

    def core_set(self, core):
        """
        get metrics by coreset

        :param core: str
        :return: dataframe
        """
        if core in self.core_groups:
            return self.data[self.data["Core"] == core]
        return None

    def to_excel(self, filename, keep_raw=True):
        """
        Save data as excel file, coreset per seprated sheet

        :param filename: filename
        :param keep_raw: bool
        :return: None
        """
        writer = pd.ExcelWriter(filename)

        for group in self.core_groups:
            label = "Core %s" % group
            df = self.core_set(group)
            del (df["Core"])
            df.to_excel(writer, sheet_name=label, index=False)

        if keep_raw:
            self.data.to_excel(writer, sheet_name="raw", index=False)
        writer.close()

    def plot(self, output_file=None, coreset=None, summary=False, charts=None):
        """
        Draw diagrams for pqos data

        :param output_file: filename
        :param coreset: list coreset need to plot, all sets as default
        :param summary: bool plot bars or curves
        :param charts:  datasets for plotting by index
        :return: None
        """
        if coreset is None:
            coreset = self.core_groups

        if charts is None:
            charts = self.data_indexes
        # elif type(charts) != type(list):
        #     charts = list(charts)

        fig = plt.figure(figsize=(16, 9), dpi=120)  # 16:9 as screen resolution
        position = 0
        chart_rows = len(charts) // 2 + 1

        for chart in charts:
            position += 1
            ax = fig.add_subplot(chart_rows, 2, position)  # 3 rows, 2 columns
            ax.set_title(chart)

            if summary:
                # plot bar diagram for aggregated data
                values = self.aggregate[chart]
                ax.bar(values.index, values.values)

            else:
                # else, plot as timeserail curves
                for cores in coreset:
                    values = self.core_set(cores)
                    ax.plot(values[chart], label=cores)

                legend = ax.legend(loc='best')
                frame = legend.get_frame()
                frame.set_alpha(1)
                # frame.set_facecolor('none')

        fig.subplots_adjust(wspace=0.3, hspace=0.4)
        if output_file is not None:
            plt.savefig(output_file)
        else:
            plt.show()

        plt.close('all')

    @property
    def grouped_data(self):
        """
        all metrics data grouped by coresets

        :return: list [(coreset_name, [data])]
        """
        return self.data.groupby("Core")

    @property
    def core_groups(self):
        """
        All coreset names

        :return: list [(str) coreset]
        """
        return list(self.data["Core"].drop_duplicates())

    @property
    def aggregate(self):
        return self.data.groupby("Core").mean()


class PQoSCSVReader(BasePQoSReader):
    """
    Through cmd "pqos -u csv -o <filename.csv>", pqos may save metrics to csv.
    """

    def read_file(self):
        self.data = pd.read_csv(self.filename)


class PQoSXMLReader(BasePQoSReader):
    """
    Through cmd "pqos -u xml -o <filename.csv>", pqos may save metrics to xml.
    """

    mapping = {"time": "Time", "core": "Core", "ipc": "IPC",
               "llc_misses": "LLC Misses", 'l3_occupancy_kB': "LLC[KB]",
               'mbm_local_MB': "MBL[MB/s]", "mbm_remote_MB": "MBR[MB/s]"}

    def read_file(self):
        file_content = ET.ElementTree(file=self.filename)
        data = []
        for metric in file_content.getroot():
            # metric = {e.tag: float(e.text) for e in metric}
            # self.data = pd.DataFrame(data).rename(columns=self.mapping)
            entry = {}
            for e in metric:
                label = self.mapping[e.tag]
                if e.tag == "time":
                    entry[label] = e.text
                else:
                    entry[label] = float(e.text)

            data.append(entry)

        self.data = pd.DataFrame(data)


class PQoSOutputReader(BasePQoSReader):
    """
    Through cmd "pqos", pqos may print metrics to stdout.
    """

    mapping = ['Core', 'IPC', 'LLC Misses', 'LLC[KB]', 'MBL[MB/s]',
               'MBR[MB/s]']

    reg = re.compile(r"(\d+)(k|m)?", re.I)

    def read_file(self):
        with open(self.filename) as fd:
            contents = fd.readlines()
        data = []
        for row_num, row_contents in enumerate(contents):
            if re.match(r"^TIME", row_contents):
                time_stamp = row_contents[5:]
            elif re.match(r"^\s*\d+", row_contents) is None:
                continue
            else:
                entry = dict(
                    zip(self.mapping, row_contents.split()))

                entry["Time"] = time_stamp
                entry["LLC Misses"] = self._llc_unit(entry["LLC Misses"])

                for key in ['IPC', 'LLC[KB]', 'MBL[MB/s]', 'MBR[MB/s]']:
                    entry[key] = float(entry[key])

                data.append(entry)

        self.data = pd.DataFrame(data)

    def _llc_unit(self, s):

        reg_group = self.reg.search(s)
        if not reg_group:
            return -1

        reg_group = reg_group.groups()
        value = int(reg_group[0])
        unit = reg_group[1]

        if unit == "k":
            return value * 1E3

        if unit == "m":
            return value * 1E6

        return value
