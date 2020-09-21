import os

from DataReader.Emon import EMONSummaryData
from DataReader.helper import CSVCombineHelper, ExcelSheet


class ColumnFormatedCombineHelper(CSVCombineHelper):
    def get_column_name(self, full_path):
        return os.path.basename(full_path)

    def get_dataframe(self, sort=None):
        return super().get_dataframe(sort_keys)


class EmonSystemView95Combine(ColumnFormatedCombineHelper):
    builder = EMONSummaryData

    def data_prepare(self, data_entry):
        return data_entry.system_view["95th percentile"]


class EmonSystemViewCombine(ColumnFormatedCombineHelper):
    builder = EMONSummaryData

    def data_prepare(self, data_entry):
        return data_entry.system_view["aggregated"]


class EmonSocketViewCombine(ColumnFormatedCombineHelper):
    builder = EMONSummaryData
    socket = 1

    def set_socket(self, socket):
        self.socket = socket

    def data_prepare(self, data_entry):
        return data_entry.socket_view["socket %s" % self.socket]


class EmonCoreViewCombine(ColumnFormatedCombineHelper):
    builder = EMONSummaryData
    socket = 1
    cores = range(0)

    def set_core(self, socket, core):
        self.core = core
        self.socket = socket

    def data_prepare(self, data_entry):
        if len(self.core) == 1:
            core = self.core[0]
            return data_entry.core_view[
                "socket %s core %s" % (self.socket, core)]
        else:
            headers = ["socket %s core %s" % (self.socket, i) for i in
                       self.cores]
            df = data_entry.core_view[headers]
            return df.T.mean()

sort_keys = None
# def sort_keys(a):
#     a = a.split("_")
#     return (int(a[1]) << 8) + int(a[0], 16)


def data_summary(path):
    print("Begin to analysis results: %s " % path)
    filename = "%s_summary.xlsx" % path
    spare_sheet = ExcelSheet(filename)

    emon_system_view = EmonSystemView95Combine(path)
    spare_sheet.add_sheet(emon_system_view.data, "System-95")

    emon_system_view = EmonSystemViewCombine(path)
    spare_sheet.add_sheet(emon_system_view.data, "System-avg")

    for socket in [0, 1]:
        emon_socket_view = EmonSocketViewCombine(path)
        emon_socket_view.set_socket(socket)
        spare_sheet.add_sheet(emon_socket_view.data, "socket%s" % socket)
        #
        # for core in (range(12, 24), range(0, 11)):
        #     emon_core_view = EmonCoreViewCombine(path)
        #     emon_core_view.set_core(socket, core)
        #     sheet_name = "Socket%s-Core%s-%s" % (socket, core[0], core[-1])
        #     spare_sheet.add_sheet(emon_core_view.data, sheet_name)
    spare_sheet.writer.close()


if __name__ == "__main__":
    path = r"."
    data_summary(path)
