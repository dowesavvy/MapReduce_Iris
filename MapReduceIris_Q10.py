#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")

class MRProb2_3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sepW_by_species,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_sepW_by_species(self, _, line):
        data = DATA_RE.findall(line)

        if len(data) < 5:
            return

        species = data[4]
        sep_W = float(data[1])

        yield species, sep_W

    def reducer_get_avg(self, key, values):
        size, total = 0, 0
        for val in values:
            size += 1
            total += val

        yield (f"{key} sepal width avg", round(total / size, 3))

if __name__ == '__main__':
    MRProb2_3.run()

