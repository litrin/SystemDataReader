import os
import re
import pandas as pd


class CSVCombineHelper(object):
    builder = None
    file_list = []
    data_set = None

    def __init__(self, path, builder=None):
        self.builder = builder
        for f in os.listdir(path):
            if f.find(".") != -1:
                continue
            self.file_list.append(os.path.join(path, f))

    def build_data_object(self, path):
        return self.builder(path)

    def select_data(self, data_entry):
        return data_entry

    def dump_excel(self, filename="data.xls"):
        df = self.get_dataframe()
        df.to_excel(filename)

    def get_dataframe(self):
        result = {}
        for path in self.file_list:
            reader = self.build_data_object(path)
            data = self.select_data(reader)

            label = os.path.split(path)[-1]
            result[label] = data
        result = pd.DataFrame(result)
        return result


class CPUCoreList(object):
    """
    convert cpu-set strings to list
    or
    Simplify cpu-set strings
    """
    __slot__ = None

    cpu_list = []
    sep = ","

    def __init__(self, cpu_set, sep=","):
        """
        Initial class

        :param cpu_set: string
        :param sep: string
        """
        if isinstance(cpu_set, list):
            cpu_set = sep.join([str(core) for core in cpu_set])

        cpu_set = cpu_set.replace(" ", "")

        # validate input string
        regex = r"^(\d+(,|-))+(\d+|),*$"
        if not re.match(regex, cpu_set):
            raise EnvironmentError(
                "string: '%s' is not a regular core list" % cpu_set)
        else:
            self.convert(cpu_set)
            self.sep = sep

    def convert(self, cpu_set):
        cpus = set()
        for element in cpu_set.split(self.sep):
            if len(element) is 0:
                continue
            if element.find("-") is not -1:
                element = element.split("-")
                core_num_end = int(element[1])
                if core_num_end < min:
                    core_num_end, min = min, core_num_end

                for core in range(min, core_num_end + 1):
                    cpus.add(core)
            else:
                cpus.add(int(element))

        self.cpu_list = list(cpus)
        self.cpu_list.sort()

    def get_list(self):
        """
        Return back a list for core#

        :return: list
        """
        return self.cpu_list

    @property
    def simplified(self):
        def format_serials(group):
            if len(group) > 2:
                cpus = "%s-%s" % (group[0], group[-1])
            else:
                cpus = self.sep.join([str(i) for i in group])
            return cpus

        cpus = ""
        group = [self.cpu_list[0]]
        for core in self.cpu_list[1:]:
            if core - group[-1] is 1:
                group.append(core)
                continue
            cpus += format_serials(group)
            cpus += self.sep
            group = [core]

        cpus = "%s%s" % (cpus, format_serials(group))
        return cpus

    def __str__(self):
        return self.simplified

    def __len__(self):
        return len(self.cpu_list)

    def __iter__(self):
        for core in self.cpu_list:
            yield core
