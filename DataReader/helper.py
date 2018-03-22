import os
import re
import pandas as pd


class CSVCombineHelper(object):
    """
    A framework to list all child paths, combine data to a data frame.
    """
    builder = None
    file_list = []
    data_set = None

    def __init__(self, main_path, builder=None):
        """
        object initialize

        :param main_path: str the main data path
        :param builder: run able data frame builder
        """
        self.builder = builder
        for f in os.listdir(main_path):
            abs_name = os.path.join(main_path, f)
            if os.path.isfile(abs_name):
                continue

            self.file_list.append(abs_name)

    def build_data_object(self, path):
        """
        help to create a data frame object, may override by child objects.

        :param path: full filename/path
        :return: object data frame object
        """
        return self.builder(path)

    def data_prepare(self, data_entry):
        """
        interface for override.
        Allow developers to do some data operation here.

        :param data_entry: data frame object
        :return: data frame object
        """
        return data_entry

    def dump_excel(self, filename="data.xls"):
        """
        Save data file as excel.

        :param filename: str filename
        :return: None
        """
        df = self.get_dataframe()
        df.to_excel(filename)

    def get_dataframe(self):
        """
        method for get data

        :return: data frame object
        """
        result = {}
        for path in self.file_list:
            label = self.get_column_name(path)

            reader = self.build_data_object(path)
            data = self.data_prepare(reader)

            result[label] = data

        result = pd.DataFrame(result)
        return result

    def get_column_name(self, full_path):
        """
        covert column name by path

        :param full_path: str abs filename
        :return: str column label
        """
        return os.path.split(full_path)[-1]


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


class ExcelSheet:
    writer = None
    sheet_number = 0

    def __init__(self, filename):
        self.writer = pd.ExcelWriter(filename)

    def __del__(self):
        self.writer.close()

    def add_sheet(self, df, sheet_label=None):
        self.sheet_number += 1
        if sheet_label is None:
            sheet_label = "sheet%s" % self.sheet_number

        df.to_excel(self.writer, sheet_name=sheet_label)
