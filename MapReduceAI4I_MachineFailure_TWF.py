from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MRCountRecordsWithValueOne(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_machine_failure_and_TWF,
                   reducer=self.reducer_count_records_with_value_one)
        ]

    def mapper_extract_machine_failure_and_TWF(self, _, line):
        # Assuming the format is: Machine Failure, TWF
        data = line.strip().split(',')  # Assuming comma-separated values
        if len(data) >= 2:
            machine_failure = data[8].strip()
            try:
                TWF = int(data[9])
                yield (machine_failure, TWF)
            except ValueError:
                pass

    def reducer_count_records_with_value_one(self, machine_failure, TWF):
        # Count the number of records with value 1
        count = sum(1 for value in TWF if value == 1)
        yield (machine_failure, count)

if __name__ == '__main__':
    MRCountRecordsWithValueOne.run()
