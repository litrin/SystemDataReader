import sys
import os
import re


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
                raise NameError(
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
