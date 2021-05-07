from abc import abstractmethod

import matplotlib.pyplot as plt
import pandas as pd

from DataReader.base import RawDataFileReader


class BasePtuReader(RawDataFileReader):
    def __init__(self, filename):
        self.filename = filename

    def set_header(self, header):
        self.column_name = header

    @abstractmethod
    def get_data(self, keyword):
        pass

    def __getattr__(self, item):
        return self.get_data(item)

    def __getitem__(self, item):
        return self.get_data(item)


class PtumonSKX(BasePtuReader):
    """
    CLI: ./ptumon -csv > filename.csv
    """
    __version__ = 1.4
    column_name = ["Time", "Dev", "Cor", "Thr", "MC", "Ch", "Sl", "CFreq",
                   "UFreq", "T", "%Util", "IPC", "PS", "%C0", "%C1", "%C6",
                   "%PC2", "%PC6", "DTS", "Temp", "Volt", "Power", "TDP",
                   "TStat", "TLog", "#TL", "TMargin", "Index"]

    def get_data(self, keyword):
        reg = r"^\d{6}\.\d{3}_\d+\s*\,%s" % keyword.upper()
        data_entries = self.egrep(reg)
        csv_body = []
        for i in data_entries:
            row = i.split(",")
            row = dict(zip(self.column_name, row))

            row["Time"], row["Index"] = tuple(row["Time"].split("_"))

            csv_body.append(row)

        data = pd.DataFrame(csv_body)
        return data.apply(pd.to_numeric, errors='ignore')


class PtumonICX(BasePtuReader):
    """
    cli: ./ptu -mon -csv > filename.csv
    """
    __version__ = 2.0
    column_name = ["Index", "Device", "Cor", "Thr", "CFreq", "UFreq", "Util",
                   "IPC", "C0", "C1", "C6", "PC2", "PC6", "MC", "Ch", "Sl",
                   "Temp", "DTS", "Power", "Volt", "TStat", "TLog", "#TL",
                   "TMargin"]

    def get_data(self, keyword):
        reg = r"^\s*\d+\s*%s" % keyword.upper()
        data_entries = self.egrep(reg)

        csv_body = []
        for i in data_entries:
            row = i.split()
            row = dict(zip(self.column_name, row))
            csv_body.append(row)

        data = pd.DataFrame(csv_body)
        return data.apply(pd.to_numeric, errors='ignore')


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
            ax.plot(data.Cores, data["CFreq(exp)"], color="red",
                    linestyle="--", linewidth=0.75)
            ax.plot(data.Cores, data["CFreq(act)"], linewidth=1)

            ax.fill_between(data.Cores, data["CFreq(exp)"], data["CFreq(act)"],
                            facecolor='r', alpha=0.3)
            ax.grid(True)

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
