import os
import re


class BaseCluceneReader:
    qps = 0
    latencies = []

    total_run_time = 0

    distribution_list = [3E4, 5E4, 7E4, 1E5, 1.5E5, 2E5]

    def __init__(self):
        if self.total_run_time is not 0:
            self.qps = len(self.latencies) / float(self.total_run_time)

    def __len__(self):
        return len(self.latencies)

    @property
    def average_latency(self):
        return sum(self.latencies) * 1.0 / len(self)

    @property
    def distribution(self):
        distribution = [0] * len(self.distribution_list)

        for latency in self.latencies:
            for offset, value in enumerate(self.distribution_list):
                if latency < value:
                    distribution[offset] += 1

        return zip(self.distribution_list, distribution)

    def threshold(self, value):
        pos = 0.0
        for latency in self.latencies:
            if latency < value:
                pos += 1

        return pos / len(self)


class CluceneSingleFileReader(BaseCluceneReader):

    def __init__(self, filename, duration=None):
        self.read_log_file(filename)

        if duration is not None:
            self.total_run_time = duration

    def read_log_file(self, filename):
        with open(filename) as fp:
            for line in fp:
                line = line.strip('\n')
                vec = line.split(' ')
                self.latencies.append(float(vec[-1]))


class CluceneLogPathReader(BaseCluceneReader):
    filename_reg = re.compile(r"^log_\d+\.log$")
    log_entry_reg = re.compile(r"Search took")

    def __init__(self, path, duration=None):
        self.read_path(path)
        if duration is not None:
            self.total_run_time = duration

    def read_path(self, path):
        for filename in os.listdir(path):

            # here assume all log files match "log_*.log"
            if self.filename_reg.match(filename):

                with open(filename) as fp:
                    for line in fp:
                        if not self.log_entry_reg.match(line):
                            continue

                        line = line.strip('\n')
                        vec = line.split(' ')
                        self.latencies.append(float(vec[-1]))
