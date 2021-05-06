import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from DataReader.Emon import EMONSummaryData, TopDownHelper, EMONDetailData


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

        plt.close()

    def save_pdf(self, filename):
        with PdfPages(filename) as pdf:
            pdf.savefig()
        plt.close()


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
            self.get_chart(ax, ts_data)

    def get_chart(self, ax, ts_data):
        tmp = self.data[ts_data]
        tmp.plot_average(ax=ax, fontsize=6)

        ax.set_xlabel("ts")
        ax.set_title(ts_data)

    def save_pdf(self, filename):
        with PdfPages(filename) as pdf:
            for ts_data in self.data.columns.values:
                fig = plt.figure()
                ax = fig.add_subplot(1, 1, 1)
                self.get_chart(ax, ts_data)
                pdf.savefig()

                plt.close()


class BaseTMAMPlot(TopDownHelper, BaseVisualization):
    map = {}

    def summary(self):
        data = self.filter(self.map["summary"])
        data = data.sort_values()

        ax = self.get_ax()
        ax.set_title("Top breakdown")

        data.plot_average.pie(ax=ax, autopct='%1.1f%%',  # shadow=True,
                              startangle=90, fontsize=6,
                              # explode=(0, 0, 0, 0.1)
                              )

        ax.set_ylabel("")

    def backend_bound(self):
        data = self.filter(self.map["backend"])

        data = data.sort_values()
        ax = self.get_ax()
        ax.set_title("Backend breakdown")

        data.plot_average.pie(ax=ax, autopct='%1.1f%%',  # shadow=True,
                              startangle=90, fontsize=6, )

        ax.set_ylabel("")

    def cache_hierarchy(self):
        data = self.filter(self.map["memory"])
        ax = self.get_ax()

        ax.set_title("Cache Hierarchy")

        data.plot_average.pie(ax=ax, autopct='%1.1f%%',  # shadow=True,
                              startangle=90, fontsize=6,
                              # explode=(0, 0, 0, 0.1)
                              )
        ax.set_ylabel("")

    def ports(self):
        data = self.filter(self.map["ports"])

        ax = self.get_ax()
        ax.set_title("Port utilizations(%)")
        data.plot_average.bar(ax=ax, fontsize=6, )

        ax.grid(True)
        ax.set_ylim(0, 100)
        ax.set_xlabel("")
        ax.set_ylabel("%")


class TMAMPlotSKX(BaseTMAMPlot):
    EDP_VERSION = "edp3.9skx_clx"
    map = {
        "summary": {"metric_TMAM_Frontend_Bound(%)": "Frontend Bound",
                    "metric_TMAM_Backend_bound(%)": "Backend Bound",
                    "metric_TMAM_Bad_Speculation(%)": "Bad Speculation",
                    "metric_TMAM_Retiring(%)": "Retiring",
                    },
        "backend": {
            "metric_TMAM_..Memory_Bound(%)": "Memory Bound",
            "metric_TMAM_..Core_Bound(%)": "Core Bound"
        },
        "memory": {
            "metric_TMAM_....L1_Bound(%)": "L1 Bound",
            "metric_TMAM_....L2_Bound(%)": "L2 Bound",
            "metric_TMAM_....L3_Bound(%)": "L3 Bound",
            "metric_TMAM_....MEM_Bound(%)": "DRAM Bound",
        },
        "ports": {
            "metric_TMAM_....Ports_Utilization(%)": "All",
            "metric_TMAM_....Divider(%)": "Devider",
            "metric_TMAM_......0_Ports_Utilized(%)": "Port 0",
            "metric_TMAM_......1_Port_Utilized(%)": "Port 1",
            "metric_TMAM_......2_Ports_Utilized(%)": "Port 2",
            "metric_TMAM_......3m_Ports_Utilized(%)": "Port 3m"
        }

    }

    @staticmethod
    def plot_all(path, image_name=None):
        if image_name is None:
            image_name = "%s.png" % path

        emon = EMONSummaryData(path)
        emon = emon.system_view

        tman_ploter = TMAMPlotSKX(emon.aggregated)
        tman_ploter.set_fig(2, 2, image_name)

        tman_ploter.summary()
        tman_ploter.cache_hierarchy()
        tman_ploter.backend_bound()
        tman_ploter.ports()

        tman_ploter.close()


class TMAMPlotICX(BaseTMAMPlot):
    EDP_VERSION = "edp4.1icx"
    prefix = "metric_TMA"
    map = {
        "summary": {"metric_TMA_Frontend_Bound(%)": "Frontend Bound",
                    "metric_TMA_Backend_bound(%)": "Backend Bound",
                    "metric_TMA_Bad_Speculation(%)": "Bad Speculation",
                    "metric_TMA_Retiring(%)": "Retiring",
                    },
        "backend": {
            "metric_TMA_..Memory_Bound(%)": "Memory Bound",
            "metric_TMA_..Core_Bound(%)": "Core Bound"
        },
        "memory": {
            "metric_TMA_....L1_Bound(%)": "L1 Bound",
            "metric_TMA_....L2_Bound(%)": "L2 Bound",
            "metric_TMA_....L3_Bound(%)": "L3 Bound",
            "metric_TMA_....DRAM_Bound(%)": "DRAM Bound",
        },

        "ports": {
            "metric_TMA_....Ports_Utilization(%)": "All",
            "metric_TMA_....Divider(%)": "Devider",
            'metric_TMA_......Ports_Utilized_0(%)': "Port 0",
            "metric_TMA_......Ports_Utilized_1(%)": "Port 1",
            "metric_TMA_......Ports_Utilized_2(%)": "Port 2",
            "metric_TMA_......Ports_Utilized_3m(%)": "Port 3m"
        }

    }

    @staticmethod
    def plot_all(path, image_name=None):
        if image_name is None:
            image_name = "%s.png" % path

        emon = EMONSummaryData(path)
        emon = emon.system_view

        tman_ploter = TMAMPlotICX(emon.aggregated, "metric_TMA")
        tman_ploter.set_fig(2, 2, image_name)

        tman_ploter.summary()
        tman_ploter.cache_hierarchy()
        tman_ploter.backend_bound()
        tman_ploter.ports()

        tman_ploter.close()


class MetricsDiagrams:
    path = None

    def __init__(self, path, view=EMONDetailData.SOCKET):
        self.path = path
        reader = EMONDetailData(path)
        self.data = reader.get_file_content(view)

    def select_data(self, metric):
        heads = {}
        for col in self.data.columns.values:
            if col.startswith(metric):
                heads[col] = col[7:]
        df = self.data[heads.keys()]
        return df.rename(columns=heads)

    def plot(self, metric):
        data = self[metric]
        data.plot()
        plt.show()

    def __getitem__(self, item):
        return self.select_data(item)

    def save_ts_diagram(self, filename):
        col = EMONSummaryData(self.path).system_view.index.values
        columns = filter(lambda a: a.startswith("metric_"), col)

        with PdfPages(filename) as pdf:
            for metric in columns:
                fig = plt.figure()
                ax = fig.add_subplot(1, 1, 1)

                data = self[metric]
                data.plot(ax=ax)
                ax.set_xlabel("ts")
                ax.set_ylabel("value")
                ax.set_title(metric)

                pdf.savefig()
                plt.close(fig)
