import re

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

from DataReader.base import RawDataFileReader


class PtuReader:
    """
    This is the latest version for ptu read, which will replace all others
    """
    filename = None
    column_name = None

    header_reg = re.compile(r"^\s?Index")
    skip_footer = 3
    separator = r"\s+"

    def __init__(self, filename, header_reg=None, skip_footer=None):
        self.filename = filename
        if header_reg is not None:
            self.header_reg = re.compile(header_reg)
        if skip_footer is not None:
            self.skip_footer = skip_footer

        # skip the additional info at the header
        fd = open(filename, "r")
        row = ""
        offset = 0
        while not self.header_reg.match(row):
            offset = fd.tell()
            row = fd.readline()

        fd.seek(offset)
        df = pd.read_table(fd,
                           sep=self.separator, skipfooter=self.skip_footer,
                           engine="python")

        self.column_name = df.columns
        for col in self.column_name:
            if col == "Device":
                continue
            # df.loc[df[col] == "-", col] = None
            df[col] = pd.to_numeric(df[col], errors="coerce")
        self.data = df

    @property
    def devices(self):
        return self.data["Device"].unique()

    def get_data(self, keyword):
        return self.data[self.data["Device"] == keyword]

    def get_core(self, cpu, core):
        df = self["CPU%s" % cpu]
        return df[df["Cor"] == int(core)]

    def get_thread(self, cpu, core, thread):
        df = self.get_core(cpu, core)
        return df[df["Thr"] == int(thread)]

    def __getattr__(self, item):
        return self.get_data(item)

    def __getitem__(self, item):
        return self.get_data(item)

    def get_telemetry(self, devices, column_name):
        _tmp = {}
        min_length = 2 ** 63
        for i in devices:
            data = self.get_data(i)
            if min_length > len(data):
                min_length = len(data)
            _tmp[i] = data[column_name].values
        _tmp = {k: v[:min_length] for k, v in _tmp.items()}  # length alignment
        df = pd.DataFrame(_tmp)
        return df

    def plot_telemetry(self, devices, column_name, method=plt.show):
        fig, ax = plt.subplots()

        df = self.get_telemetry(devices, column_name)
        df.plot(ax=ax)
        ax.set_title(column_name)

        fig.tight_layout()
        method()


    def save_pdf(self, filename, devices=None, telemetries=None):
        if devices is None:
            devices = list(filter(lambda a: a.startswith("CPU"), self.devices))

        if telemetries is None:
            telemetries = ['CFreq', 'Util', 'IPC',
                           'Temp', 'Power',
                           'C0', 'C1']

        with PdfPages(filename) as pdf:
            for p in telemetries:
                self.plot_telemetry(devices=devices, column_name=p,
                                    method=pdf.savefig)


PtumonSKX = PtuReader
PtumonICX = PtuReader

Ptumon1 = PtumonSKX
Ptumon2 = PtumonICX


class PtuTurboFrequency(RawDataFileReader):
    """
    cli: ./ptu -ct 8 -avx [1|2|3] > filename.txt
    """
    __version__ = 2.0
    HEADER = ['Instr', 'CPU', 'Cores', 'CFreq(act)', 'CFreq(exp)', 'UFreq',
              'Util(act)', 'Util(exp)', 'Power', 'TDP', 'Temp', 'Volt',
              'UVolt']
    Instructions = None
    data = None

    def __init__(self, filename):
        self.filename = filename
        self.read_file()

    def read_file(self):
        capture = False
        data = []
        for row in self.reader():
            if len(row) < 2:
                continue
            if row.startswith("### TURBO is enabled ###"):
                capture = True
                continue

            if capture:
                row = row.split()
                if len(row) < 13:
                    continue
                if "CPU" in row:
                    continue
                else:
                    self.Instructions = row[0]
                    for i, v in enumerate(row):
                        if i == 0:
                            continue
                        if i == 1 or i == 2:
                            row[i] = int(v)
                            continue
                        row[i] = float(v)

                    data.append(dict(zip(self.HEADER, row)))

        df = pd.DataFrame(data)
        self.data = df

    def get_by_cpu(self, cpu):
        return self.data[self.data.CPU == cpu]

    def get_by_cores(self, cores):
        return self.data[self.data["Cores"] == cores]

    def plot_all(self, filename=None):
        core_list = self.data["CPU"].unique()
        fig, axs = plt.subplots(len(core_list), 1)

        for cpu in self.data["CPU"].unique():
            data = self.get_by_cpu(cpu)
            ax = axs[cpu]
            ax.plot_telemetry(data.Cores, data["CFreq(exp)"], color="red",
                              linestyle="--", linewidth=0.75)
            ax.plot_telemetry(data.Cores, data["CFreq(act)"], linewidth=1)

            ax.fill_between(data.Cores, data["CFreq(exp)"], data["CFreq(act)"],
                            facecolor='r', alpha=0.3)

            # ax.grid(True)

            ax.set_title("Socket %s (%s)" % (cpu, self.Instructions),
                         fontsize=10)
            ax.set_xlabel("Cores", fontsize=8)
            ax.set_ylabel("MaxFreq(GHz)", fontsize=8)
            ax.autoscale_view(True)

        fig.tight_layout()

        if filename is None:
            plt.show()
        else:
            plt.savefig(filename, dpi=600, transparent=True, pad_inches=0)
        plt.close()

    def __getitem__(self, item):
        return self.get_by_cpu(item)

