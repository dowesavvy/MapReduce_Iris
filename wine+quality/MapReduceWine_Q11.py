#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWineQuality(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_avg_alcohol_by_quality,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_avg_alcohol_by_quality(self, _, line):
        # Skip header row
        if line.startswith("fixed"):
            return

        # Wine dataset is semicolon separated
        parts = line.strip().split(";")

        # Ensure correct number of columns
        if len(parts) < 12:
            return

        try:
            alcohol = float(parts[10])   # the alcohol column
            quality = parts[11]          # the quality column

            # Key = quality score
            # Value = alcohol level
            yield quality, alcohol

        except ValueError:
            return

    def reducer_get_avg(self, key, values):
        total = 0
        count = 0

        for v in values:
            total += v
            count += 1

        if count > 0:
            avg = total / count
            yield f"Quality {key} average alcohol", round(avg, 3)


if __name__ == "__main__":
    MRWineQuality.run()

