from mrjob.job import MRJob
from mrjob.step import MRStep

class FrequentItemsJob(MRJob):
    def mapper(self, _, line):
        items = line.split(',')
        for item in items:
            yield (item, 1)

    def reducer(self, key, values):
        total_count = sum(values)
        if total_count >= 2:  # Ajusta el umbral según tus necesidades
            yield None, (key, total_count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    FrequentItemsJob.run()