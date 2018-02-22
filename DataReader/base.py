import sys
import re


class RawDataFileReader(object):
    """
    Base class to read file
    """
    filename = sys.stdin
    _cache = None

    def reader(self, refresh=False):
        """
        file reader
        :param refresh: mandatory refresh the cached file
        :return: list
        """
        if refresh or self._cache is None:
            with open(self.filename, "r") as fd:
                self._cache = fd.readlines()

        return self._cache

    def grep(self, key_word):
        """
        Linux grep command
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
