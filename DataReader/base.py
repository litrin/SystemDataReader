import sys
import re


class RawDataFileReader(object):
    """
    Base class to read file
    """
    filename = sys.stdin

    def reader(self):
        """
        file reader
        :return: list
        """
        with open(self.filename, "r") as fd:
            return fd.readlines()

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
