#!/usr/bin/env python3:n


import optparse

from DataReader.Emon import EMONSummaryData
from DataReader.Emon_visual import TMAMPlotSKX, TMAMPlotICX


def get_data(path, view, socket=0, core=0):
    view = view.lower()
    data = EMONSummaryData(path)
    if view == "core":
        filter = "socket %s core %s" % (socket, core)
        data = data.core_view[filter]
    elif view == "socket":
        filter = "socket %s" % socket
        data = data.socket_view[filter]
    else:
        data = data.system_view["aggregated"]

    # print(data)

    return data


PLOT_HELPER = {
    "SKX": TMAMPlotSKX,
    "ICX": TMAMPlotICX
}


def plot_data(data, platform, filename):
    platform = platform.upper()
    if platform not in PLOT_HELPER.keys():
        return

    tman_ploter = PLOT_HELPER[platform](data,
                                        prefix=PLOT_HELPER[platform].prefix)

    tman_ploter.set_fig(2, 2, filename)

    tman_ploter.summary()
    tman_ploter.cache_hierarchy()
    tman_ploter.backend_bound()
    tman_ploter.ports()

    tman_ploter.close()


def ui():
    optParser = optparse.OptionParser()
    optParser.add_option('-e', '--emon', action='store', type="string",
                         help='EMON data path or EMON data excel filename',
                         default=".", dest="path", metavar="FILE")

    optParser.add_option('-o', '--output_file', type="string",
                         help='Output file (PNG images) filename',
                         default="tmam.png", metavar="FILE", dest="img")

    optParser.add_option('-s', '--socket', action='store', type="int",
                         help='TMAM of socket #  ',
                         default="-1", dest="socket")

    optParser.add_option('-c', '--core', action='store', type="int",
                         help='TMAM of core #  of socket # ',
                         default="-1", dest="core")

    optParser.add_option('-g', '--code_name', action='store', type="string",
                         help='CPU code name, [SKX|ICX]',
                         default="SKX", dest="platform")

    option, args = optParser.parse_args()

    if option.core != -1:
        view = "core"
    elif option.socket != -1:
        view = "socket"
    else:
        view = "system"

    data = get_data(option.path, view, option.socket, option.core)
    plot_data(data, option.platform, option.img)

    exit()


if __name__ == "__main__":
    ui()