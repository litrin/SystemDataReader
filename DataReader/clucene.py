class CluceneLatency:
    latencies = []
    qps = 0

    def __init__(self, filename, duration=None):
        with open(filename) as fp:
            for line in fp:
                line = line.strip('\n')
                vec = line.split(' ')
                self.latencies.append(float(vec[-1]))

        if duration is not None:
            self.qps = len(self.latencies) / float(duration)

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
