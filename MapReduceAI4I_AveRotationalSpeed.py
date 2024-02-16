from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MRProb2_3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ave_rotational_speed,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_ave_rotational_speed(self, _, line):
        # yield each rotational speed
        data = line.strip().split(',')  # Assuming comma-separated values
        if len(data) >= 2:
            try:
                rotational_ave = float(data[5]) #column position in dataset
                yield ("Rotational speed [rpm]", rotational_ave)
            except ValueError:  #dealing with errors during processing
                pass


    def reducer_get_avg(self, key, values):
        # get average of the rotational speed
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield ("Type Rotational speed [rpm] avg", round(total / size, 1)) #rounded to 1 decimal place

if __name__ == '__main__':
    MRProb2_3.run()
