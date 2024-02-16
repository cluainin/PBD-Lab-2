from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MRProb2_3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ave_process_temp,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_ave_process_temp(self, _, line):
        # yield each average process temperature K
        data = line.strip().split(',')  # Assuming comma-separated values
        if len(data) >= 2:
            try:
                process_temperature_ave = float(data[4])
                yield ("Process temperature [K]", process_temperature_ave)
            except ValueError:  #dealing with errors during processing
                pass


    def reducer_get_avg(self, key, values):
        # get average of the process temperature K
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield ("Process temperature [K] avg", round(total / size, 1)) #rounded to 1 decimal place

if __name__ == '__main__':
    MRProb2_3.run()
