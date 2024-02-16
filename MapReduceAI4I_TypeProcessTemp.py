from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MRProductTemperatureAverage(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_product_temperature,
                   reducer=self.reducer_calculate_average_temperature)
        ]

    def mapper_extract_product_temperature(self, _, line):
        # The format Type, Process temperature [K]
        data = line.strip().split(',')  # Assuming comma-separated values
        if len(data) >= 2:
            product_type = data[2].strip()
            try:
                temperature = float(data[4])
                yield (product_type, temperature)
            except ValueError:
                pass

    def reducer_calculate_average_temperature(self, product_type, temperatures):
        # Calculate the average temperature for the given product type
        total_temperature = 0
        count = 0
        for temperature in temperatures:
            total_temperature += temperature
            count += 1
        if count > 0:
            average_temperature = total_temperature / count
            yield (product_type, round(average_temperature, 2)) #round to 2 decimal places


if __name__ == '__main__':
    MRProductTemperatureAverage.run()
