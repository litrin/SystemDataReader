import os
import re
import sys

import pandas as pd

__all__ = ["RawDataFileReader", "DataCacheObject", "DataReaderError",
           "LinuxColumnStyleOutputReader"]


class RawDataFileReader(object):
    """
    Base class to read file
    """

    filename = sys.stdin

    # build up a cache object to accelerate the file access
    _raw_data_cache = None

    def reader(self, cache_refresh=False):
        """
        main method to read raw data files.

        :param cache_refresh: mandatory refresh the cached file
        :return: list
        """
        if cache_refresh or self._raw_data_cache is None:
            if not os.path.exists(self.filename):
                raise DataReaderError(
                    "Raw data file %s is not exist" % self.filename)
            with open(self.filename, "r") as fd:
                self._raw_data_cache = fd.readlines()

        return self._raw_data_cache

    def grep(self, key_word):
        """
        Same function from Linux grep command
        :param key_word: str
        :return: list
        """
        # return self.egrep(key_word)
        return filter(lambda a: a.find(key_word) != -1, self.reader())

    def egrep(self, regex):
        """
        This method support regex strings
        :param regex: regex string
        :return: list
        """
        return filter(lambda a: re.match(regex, a), self.reader())

    def grep_iterator(self, regex):
        """
        This method is implemented to read large files.
        :param regex: regex string
        :return: iterator
        """
        regex = re.compile(regex)
        # for row in self.reader():
        with open(self.filename, "r") as fd:
            while True:
                row = fd.readline()
                if len(row) is 0:
                    break

                if regex.match(row):
                    yield row

    def read_line(self, start, end=0):
        """
        Get content from row number
        :param start: int
        :param end: int
        :return: iterator
        """
        content = self.reader()
        return content[start - 1:end - 1]

    @property
    def create_time(self):
        return os.path.getctime(self.filename)

    @property
    def file_size(self):
        return os.path.getsize(self.filename)


class DataReaderError(BaseException):
    """
    Base error handler
    """
    pass


class DataCacheObject:
    """
    Cache data object
    """

    _data_cache = None  # the cache object with dict
    header = []

    def get_content(self):
        """
        main function to get real data, must be overrode by child!!!

        :return: data object
        """
        raise DataReaderError(
            "method get_dataframe must be overrode by child!")

    @property
    def data(self):
        """
        this is the entry what provides access interface to data cached

        :return: Data object
        """
        if self._data_cache is None:
            self._data_cache = self.get_content()

        return self._data_cache

    def __getitem__(self, item):
        if item not in self.header:
            return None

        return self.data[item]


class LinuxColumnStyleOutputReader(RawDataFileReader, DataCacheObject):
    data_row_regex = r".*"

    def __init__(self, filename, column_name_list=None):
        self.filename = filename
        self.set_column_name(column_name_list)

    def set_column_name(self, column_name_list=None):
        if column_name_list is not None:
            self.header = column_name_list

    def get_content(self):
        data = []
        for row in self.grep_iterator(self.data_row_regex):
            data.append(self.data_formatter(row))
        df = pd.DataFrame(data, columns=self.header, dtype=float)

        return df

    def data_formatter(self, row):
        return row.split()
