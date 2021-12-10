from DataReader.Emon import EMONSummaryData, TopDownHelper, EMONDetailData
from itertools import combinations

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

PIPELINE_DIAGRAM = True
try:
    from matplotlib.sankey import Sankey
except ImportError:
    PIPELINE_DIAGRAM = False


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

    def get_hist(self, ax, ts_data):
        tmp = self.data[ts_data]
        tmp.plot.hist(ax=ax, fontsize=6)

        ax.set_title("%s (hist)" % ts_data)

    def save_pdf(self, filename, with_hist=False):
        with PdfPages(filename) as pdf:
            for ts_data in self.data.columns.values:

                fig = plt.figure()
                ax = fig.add_subplot(1, 1, 1)
                self.get_chart(ax, ts_data)
                pdf.savefig()

                if with_hist:
                    fig = plt.figure()
                    ax = fig.add_subplot(1, 1, 1)
                    self.get_hist(ax, ts_data)
                    pdf.savefig()

                plt.close()


class BaseTMAMPlot(TopDownHelper, BaseVisualization):
    map = {}

    def summary(self):
        toplevel = self.filter(self.map["summary"])
        toplevel = toplevel.sort_values()
        values = toplevel * 100 / toplevel.sum()

        backendbound = self.filter(self.map["backend"])

        values["Backend (memory)"] = values['Backend Bound'] * backendbound[
            "Memory Bound"] / backendbound.sum()
        values["Backend (core)"] = values['Backend Bound'] * backendbound[
            "Core Bound"] / backendbound.sum()

        del (values['Backend Bound'])

        ax = self.get_ax()
        ax.set_title("Top breakdown", fontsize=8)

        if PIPELINE_DIAGRAM:  # pipeline diagram chat

            ax.set_frame_on(False)
            ax.set(xticks=[], yticks=[])
            values = values * -1
            sankey = Sankey(ax=ax, format='%.1f', unit='%', scale=0.0015,
                            offset=0.1,
                            flows=[100,
                                   values['Frontend Bound'],
                                   values['Backend (memory)'],
                                   values["Backend (core)"],
                                   values['Bad Speculation'],
                                   values['Retiring']],
                            labels=['', 'Frontend\nBound',
                                    'Backend\n(Memory)', 'Backend\n(Core)',
                                    'BadSpeculation', 'Retiring'],

                            orientations=[0, -1, 1, 1, -1, 0],
                            patchlabel="Pipeline\nstalled",
                            facecolor="deepskyblue"
                            )

            chat = sankey.finish()
            chat[0].text.set_fontsize(6)
            for i in chat[0].texts:
                i.set_fontsize(4)
                # i.set_fontweight("light")

            if values['Retiring'] > -30:
                chat[0].texts[-1].set_color('r')

        else:  # pi diagram chat
            values.plot.pie(ax=ax, autopct='%1.1f%%',  # shadow=True,
                            startangle=90, fontsize=6,
                            # explode=(0, 0, 0, 0.1)
                            )

            ax.set_ylabel("")

        plt.tight_layout()

    def backend_bound(self):
        data = self.filter(self.map["backend"])

        data = data.sort_values()
        ax = self.get_ax()
        ax.set_title("Backend breakdown", fontsize=8)

        data.plot.pie(ax=ax, autopct='%1.1f%%',  # shadow=True,
                      startangle=90, fontsize=6, )

        ax.set_ylabel("")
        plt.tight_layout()

    def cache_hierarchy(self):
        data = self.filter(self.map["memory"])
        ax = self.get_ax()

        ax.set_title("Cache Hierarchy", fontsize=8)

        data.plot.pie(ax=ax, autopct='%1.1f%%',  # shadow=True,
                      startangle=90, fontsize=6,
                      # explode=(0, 0, 0, 0.1)
                      )
        ax.set_ylabel("")

        plt.tight_layout()

    def ports(self):
        data = self.filter(self.map["ports"])

        ax = self.get_ax()
        ax.set_title("Port utilizations(%)", fontsize=8)
        data.plot.bar(ax=ax, fontsize=6, )

        ax.grid(True)
        ax.set_ylim(0, 100)
        ax.set_xlabel("")
        ax.set_ylabel("%")

        plt.tight_layout()


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


class TMAMPlotSPR(BaseTMAMPlot):
    EDP_VERSION = "edp4.24spr"
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
            "metric_TMA_......Ports_Utilized_3m(%)": "Port 3m",

            "metric_TMA_........ALU_Op_Utilization(%)": "ALU",

            "metric_TMA_..........Port_0(%)": "Port#0",
            "metric_TMA_..........Port_1(%)": "Port#1",
            "metric_TMA_..........Port_6(%)": "Port#6",

        }

    }

    @staticmethod
    def plot_all(path, image_name=None):
        if image_name is None:
            image_name = "%s.png" % path

        emon = EMONSummaryData(path)
        emon = emon.system_view

        tman_ploter = TMAMPlotSPR(emon.aggregated, "metric_TMA")
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
        data = reader.get_file_content(view)
        for m in data.columns.values:
            if not m.startswith("metric_"):
                del (data[m])
        self.data = data

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

    def _get_column_list(self):
        col = EMONSummaryData(self.path).system_view.index.values
        columns = filter(lambda a: a.startswith("metric_"), col)
        return columns

    def save_ts_diagram(self, filename):
        columns = self._get_column_list()

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

    def aggragate(self, filename, method="mean", **kwargs):
        columns = self._get_column_list()

        with PdfPages(filename) as pdf:
            for metric in columns:
                fig = plt.figure()
                ax = fig.add_subplot(1, 1, 1)

                data = self[metric]
                for k, headers in kwargs.items():
                    data = data[headers].agg(method, axis="columns")
                    data = data.drop(columns=headers)

                data.plot(ax=ax)
                ax.set_xlabel("ts")
                ax.set_ylabel("value")
                ax.set_title(metric)

                pdf.savefig()
                plt.close(fig)


class PCAClusteringDiagram:
    data = None
    cluster = 3
    kmeans = None
    pca_data = None

    _marks = list(zip(mcolors.TABLEAU_COLORS, Line2D.filled_markers))
    n_components = 0xffff

    def __init__(self, data, clusters: int = 3, n_comp: int = 0xffff):
        self.data = data
        self.cluster = clusters
        self.n_components = n_comp

        if len(self.dimension) < self.n_components:
            self.n_components = len(self.dimension) - 1

    def analyze(self):
        pca = PCA(n_components=self.n_components, whiten=True)
        pca.fit(self.data)
        self.pca_data = pca.transform(self.data)
        self.pc_comp = pca.components_

        self.kmeans = KMeans(self.cluster)
        self.kmeans.fit(self.pca_data)

    @property
    def dimension(self):
        return self.data.columns

    @property
    def centroids(self):
        return self.kmeans.cluster_centers_

    @property
    def cluster_labels(self):
        return self.kmeans.labels_

    def _plot_chat(self, dem1, dem2):
        fig = plt.figure()
        for i, l in enumerate(self.cluster_labels):
            markers = self._marks[l]
            name = "%s: %s" % (i, self.data.index[i])
            cluster_samples = self.pca_data[i]

            plt.plot(cluster_samples[dem1], cluster_samples[dem2],
                     color=markers[0],
                     marker=markers[1], ls='None', label=name)
            plt.annotate(i,
                         xy=(cluster_samples[dem1], cluster_samples[dem2]),
                         xytext=(
                             cluster_samples[dem1], cluster_samples[dem2]),
                         fontsize=7)

        plt.scatter(self.centroids[:, dem1], self.centroids[:, dem2],
                    color='r', marker="+")

        plt.title("PC%s + PC%s" % (dem1, dem2), fontsize=10)
        plt.xlabel("PC%s" % dem1, fontsize=8)
        plt.ylabel("PC%s" % dem2, fontsize=8)
        plt.legend(bbox_to_anchor=(1, 1), fontsize=6)

        fig.tight_layout()

    def display(self, dem1=0, dem2=1):
        self._plot_chat(dem1, dem2)
        plt.show()

    def plot_all(self, method=plt.show):
        for i in combinations(range(self.n_components), 2):
            self._plot_chat(i[0], i[1])
            yield method()
            plt.close()

    def plot_centroids(self, method=plt.show):
        col = len(self.dimension)
        y_pos = range(col)

        fig, ax = plt.subplots()
        width = 1 / col  # 0.2
        offset = 0
        for i, v in enumerate(self.pc_comp):
            offset += width
            pos = [(a - 0.5) + offset for a in y_pos]

            ax.bar(pos, v, width=width * 0.95, label="PC%s" % i)

        ax.set_xticks(y_pos)
        ax.set_xticklabels(self.dimension, fontsize=6)
        ax.set_title('Weight Value in Eigenvector')
        ax.legend()

        fig.tight_layout()

        yield method()

    def save_pdf(self, filename):
        with PdfPages(filename) as pdf:
            for _ in self.plot_all(pdf.savefig):
                pass

            for _ in self.plot_centroids(pdf.savefig):
                pass
