from mrjob.job import MRJob
from mrjob.step import MRStep

class MatrixMultiplyJob(MRJob):
    def mapper(self, _, line):
        key, matrix, i, value = line.split(',')
        i, value = int(i), float(value)
        
        if matrix == 'A':
            yield (i, ('A', key, value))
        else:
            yield (i, ('B', key, value))

    def reducer(self, key, values):
        A_values = {}
        B_values = {}
        
        for value in values:
            matrix, col, val = value
            if matrix == 'A':
                A_values[col] = val
            else:
                B_values[col] = val
        
        result = sum(A_values[col] * B_values[col] for col in A_values)
        yield key, result

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    MatrixMultiplyJob.run()