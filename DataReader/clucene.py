import os


class BaseCluceneReader:
    qps = 0
    latencies = []

    total_run_time = 0

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
        distribution_list = [3E4, 5E4, 7E4, 1E5, 1.5E5, 2E5]
        distribution = [0] * len(distribution_list)

        for latency in self.latencies:
            for offset, value in enumerate(distribution_list):
                if latency < value:
                    distribution[offset] += 1

        return zip(distribution_list, distribution)


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


class CluceneLogPathReader(CluceneSingleFileReader):

    def __init__(self, path, duration=None):
        self.read_path(path)
        if duration is not None:
            self.total_run_time = duration

    def read_path(self, path):
        for filename in os.listdir(path):
            if filename[:4] == "log_" and filename[-4:] == ".log":
                self.read_log_file(filename)
