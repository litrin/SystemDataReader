from matplotlib import pyplot as plt

from DataReader.Emon import EMONSummaryData, TopDownAnalyzer
import matplotlib.pyplot as plt


class BaseVisualization:
    fig = None
    chart = (1, 1)
    counter = 0

    filename = None

    def set_fig(self, nrows, ncols, filename=None):
        self.fig = plt.figure()
        self.chart = (nrows, ncols)

        if filename is not None:
            self.filename = filename

    def get_ax(self):
        self.counter += 1
        if self.fig is None:
            self.set_fig(1, 1)
        ax = self.fig.add_subplot(self.chart[0], self.chart[1], self.counter)

        return ax

    def close(self):
        if self.filename is None:
            plt.show()
        else:
            plt.savefig(self.filename, dpi=300, transparent=True,
                        pad_inches=0)


class TimeSerialPlot(BaseVisualization):
    length = 0

    def __init__(self, emon_detail, name):
        name = name.lower()
        keys = filter(lambda a: a.lower().find(name) != -1,
                      emon_detail.columns.values)
        self.data = emon_detail[keys]
        self.length = len(self.data.columns.values)

    def plot(self):
        if self.fig is None:
            ncols = 2
            nrows = self.length // ncols
            if self.length % ncols != 0:
                nrows += 1
            self.set_fig(nrows, ncols)

        for ts_data in self.data.columns.values:
            ax = self.get_ax()
            self.data[ts_data].plot(ax=ax)
            ax.set_xlabel("ts")
            ax.set_title(ts_data)


class TMAMPlot(TopDownAnalyzer, BaseVisualization):

    def port_utilization(self):
        keys = {
            "metric_TMAM_....Ports_Utilization(%)": "All",
            "metric_TMAM_....Divider(%)": "Devider",
            "metric_TMAM_......0_Ports_Utilized(%)": "Port 0",
            "metric_TMAM_......1_Port_Utilized(%)": "Port 1",
            "metric_TMAM_......2_Ports_Utilized(%)": "Port 2",
            "metric_TMAM_......3m_Ports_Utilized(%)": "Port 3m"
        }
        data = self.filter(keys)

        ax = self.get_ax()
        ax.set_title("Port utilizations(%)")
        data.plot.bar(ax=ax)

        ax.grid(True)
        ax.set_ylim(0, 100)
        ax.set_xlabel("")
        ax.set_ylabel("%")

    def summary(self):
        keys = {"metric_TMAM_Frontend_Bound(%)": "Frontend Bound",
                "metric_TMAM_Backend_bound(%)": "Backend Bound",
                "metric_TMAM_Bad_Speculation(%)": "Bad Speculation",
                "metric_TMAM_Retiring(%)": "Retiring",
                # "metric_TMAM_..Memory_Bound(%)": "Memory Bound(Backend)",
                # "metric_TMAM_..Core_Bound(%)": "Core Bound(Backend)"
                }
        data = self.filter(keys)
        data = data.sort_values()

        ax = self.get_ax()
        ax.set_title("Top-down break down (top)")

        data.plot.pie(ax=ax, autopct='%1.1f%%', shadow=True,
                      startangle=90,
                      explode=(0, 0, 0, 0.1))

        ax.set_ylabel("")

    def backend_bound(self):
        keys = {
            "metric_TMAM_..Memory_Bound(%)": "Memory Bound",
            "metric_TMAM_..Core_Bound(%)": "Core Bound"}

        data = self.filter(keys)

        data = data.sort_values()
        ax = self.get_ax()
        ax.set_title("Backend break down")

        data.plot.pie(ax=ax, autopct='%1.1f%%', shadow=True,
                      startangle=90, )

        ax.set_ylabel("")

    def cache_hierarchy(self):
        keys = {
            "metric_TMAM_....L1_Bound(%)": "L1 Bound",
            "metric_TMAM_....L2_Bound(%)": "L2 Bound",
            "metric_TMAM_....L3_Bound(%)": "L3 Bound",
            "metric_TMAM_....MEM_Bound(%)": "DRAM Bound",
        }

        data = self.filter(keys)
        ax = self.get_ax()
        ax.set_title("Cache Hierarchy")

        data.plot.pie(ax=ax, autopct='%1.1f%%', shadow=True,
                      startangle=90,
                      explode=(0, 0, 0, 0.1))
        ax.set_ylabel("")

    @staticmethod
    def plot_all(path, image_name=None):
        if image_name is None:
            image_name = "%s.png" % path

        emon = EMONSummaryData(path)
        emon = emon.system_view

        tman_ploter = TMAMPlot(emon.aggregated)
        tman_ploter.set_fig(2, 2, image_name)

        tman_ploter.summary()
        tman_ploter.cache_hierarchy()
        tman_ploter.backend_bound()
        tman_ploter.port_utilization()

        tman_ploter.close()
