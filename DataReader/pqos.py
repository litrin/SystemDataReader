import re

import matplotlib.pyplot as plt
import pandas as pd

from DataReader.base import RawDataFileReader, DataCacheObject


class PQoSCSVReader:
    """
    Through cmd "pqos -u csv -o <filename.csv>", pqos may save metrics to csv.
    """

    def __init__(self, filename, ts=False, total_bandwidth=False):
        self.filename = filename
        self.data = pd.read_csv(filename)

        if ts:
            # when enabled ts, covert as time serail data
            self.data["Time"] = pd.DatetimeIndex(self.data["Time"])

        if total_bandwidth:
            # if enabled, will calculate total memory bw.
            # Strongly suggest to enable it when system NUMA mode disabled.
            self.data["Total MemoryBW"] = self.data["MBL[MB/s]"] + self.data[
                "MBL[MB/s]"]

    def data_clean(self, column, fun):
        """

        :param column: str column name
        :param fun: runable a function to data clean
        :return: None
        """
        self.data[column] = fun(self.data[column])

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

    def plot(self, output_file, coreset=None):
        """
        Draw diagrams for pqos data

        :param output_file: filename
        :param coreset: list coreset need to plot, all sets as default
        :return: None
        """
        if coreset is None:
            coreset = self.core_groups

        charts = list(self.data.columns)
        fig = plt.figure(figsize=(16, 9), dpi=120)  # 16:9 as screen resolution

        position = 1
        for chart in charts[2:]:
            ax = fig.add_subplot(3, 2, position)  # 3 rows, 2 columns
            for cores in coreset:
                values = self.core_set(cores)
                ax.plot(values[chart], label=cores)

            ax.set_title(chart)
            legend = ax.legend(loc='best')
            frame = legend.get_frame()
            frame.set_alpha(1)
            # frame.set_facecolor('none')

            position += 1

        fig.subplots_adjust(wspace=0.3, hspace=0.4)
        plt.savefig(output_file)
        plt.close('all')


class PQoSReader(RawDataFileReader, DataCacheObject):
    headers = ['CORE', 'IPC', 'MISSES', 'LLC', 'MBL', 'MBR']
    sample_range = None

    def __init__(self, input, sample_range=None):
        self.filename = input
        self.sample_range = sample_range

    def get_content(self):
        data = []
        skip = 0
        for lines, entry in enumerate(self.reader()):
            if re.match(r"^\s*\d+", entry) is None:
                continue
            try:
                entry = dict(zip(self.headers, entry.split()))

                entry["IPC"] = float(entry["IPC"])
                # sometime there haven't breaks between column IPC and MISSES
                assert (entry["IPC"] < 4)  # resonable IPC value

                entry["MISSES"] = int(entry["MISSES"][:-1]) << 10

                for key in ['LLC', 'MBL', 'MBR']:
                    entry[key] = float(entry[key])
                data.append(entry)

            except AssertionError:
                print("spliting fail at %s, skip this record" % self.filename)

            except:
                skip += 1
                print("%s skip" % self.filename)

        df = pd.DataFrame(data)
        if self.sample_range is not None:
            df = df[self.sample_range[0]:self.sample_range[1]]

        return df

    @property
    def ipc(self):
        return self.data.groupby(["CORE"]).IPC

    @property
    def llc(self):
        return self.data.LLC

    @property
    def memory_bandwidth_total(self):
        data = self.data.groupby(["CORE"]).mean().sum()
        return data.MBL + data.MBR

    def compare_by_coresets(self, col_name):
        data = self.data[col_name]

        all_coresets = data.groupby("CORE").nunique()["CORE"].index
        result = {
            coreset: data[self.data["CORE"] == coreset].values
            for coreset in all_coresets
        }

        return pd.DataFrame(result)
