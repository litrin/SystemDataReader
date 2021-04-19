import pandas

from DataReader.base import RawDataFileReader


class SDEInstructionSummaryReader(RawDataFileReader):
    """
    Read output files from Intel SDE
    e.g.:
        ./sde64 -mix   -- <app>
    """
    __version__ = "8.63.0"  # Tested for this external version

    total_instruction = 0
    instructions = None
    stacks = None

    def __init__(self, filename="sde-mix-out.txt", component=None):
        self.filename = filename

        if component is None:
            self.component = "$global-dynamic-counts"
        else:
            self.component = "$dynamic-counts-for-function: %s" % component

        self.get_data()

    def get_data(self):
        key_words = "# %s" % self.component

        stack_call = {}
        instruction_call = {}
        _gatting = True  # door keeper

        for row in self.reader():
            # print(row)
            if _gatting and not row.startswith(key_words):
                continue
            _gatting = False
            if row.startswith("#"):
                continue

            name, counts = row.split()

            if row.startswith("*"):
                stack_call[name[1:]] = int(counts)
            else:
                instruction_call[name] = int(counts)

            if name == "*total":
                _gatting = True
                self.total_instruction = counts
                break

        self.instructions = pandas.DataFrame([instruction_call],
                                             index=["counts"],
                                             dtype="uint64").T

        self.stacks = pandas.DataFrame([stack_call], index=["counts"],
                                       dtype="uint64").T

    def __getitem__(self, item):

        if item.startswith("*"):
            item = item[1:]
            df = self.stacks
        else:
            item = item.upper()
            df = self.instructions

        if item not in df.index:
            return 0

        return df.loc[item].counts


class InstructionLatencyTable:
    """
    Reference latency in cycles for each instructions
    """
    MIN = 0
    MAX = 1
    AVG = 2
    RANGE = 3

    def __init__(self, filename, default_type=0, na_fill=False):
        self.set_value(default_type)
        df = pandas.read_csv(filename, index_col=[0])

        _tmp_index = [(i[0:i.find("_")], i[i.find("_") + 1:])
                      for i in df.index.values]

        df.index = pandas.MultiIndex.from_tuples(_tmp_index,
                                                 names=["name", "ext_name"])
        if na_fill:
            df = df.fillna(value=1)
        self.data = df

    def set_value(self, default_value):
        self.defaule_type = default_value

    def aggregate(self, column):
        if column not in self.data.columns:
            return None

        df = self.data[column].groupby("name")

        if self.defaule_type is self.MIN:
            return df.min()
        if self.defaule_type is self.MAX:
            return df.max()
        if self.defaule_type is self.AVG:
            return df.mean()
        if self.defaule_type is self.RANGE:
            _min, _max = df.min(), df.max()
            return pandas.DataFrame({"MIN": _min, "MAX": _max})

        return None

    @property
    def latency(self):
        return self.aggregate("Latency")

    @property
    def throughput(self):
        return self.aggregate("Throughput")

    def attache_summary_data(self, summary):
        if not isinstance(summary, SDEInstructionSummaryReader):
            raise TypeError("SDEInstructionSummaryReader object is required!")

        lat = self.latency
        ins = summary.instructions["counts"]
        return pandas.DataFrame(
            {"counts": ins, "latency(cycles)": lat, "cycles": ins * lat})

    def __mul__(self, other):
        return self.attache_summary_data(other)

# if __name__ == "__main__":
#     sde_result = SDEInstructionSummaryReader(
#         r"C:\Users\liqunjia\OneDrive - Intel Corporation\Desktop\sde-mix-out.txt")
#
#     latency_data = InstructionLatencyTable(
#         r"C:\Users\liqunjia\OneDrive - Intel Corporation\Desktop\throughput and latency.csv",
#     )
#     total_latency = latency_data * sde_result
#     total_latency.to_csv(
#         r"C:\Users\liqunjia\OneDrive - Intel Corporation\Desktop\ret.csv", )
