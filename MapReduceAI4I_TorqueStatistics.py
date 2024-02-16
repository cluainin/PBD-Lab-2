from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import numpy as np
from scipy import stats

class MRProductTorqueSummary(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_torque,
                   reducer=self.reducer_calculate_summary_statistics)
        ]

    def mapper_extract_torque(self, _, line):
        # The format Type, torque Nm
        data = line.strip().split(',')  # Assuming comma-separated values
        if len(data) >= 2:
            product_type = data[2].strip()
            try:
                torque = float(data[6])
                yield (product_type, torque)
            except ValueError:
                pass

    def reducer_calculate_summary_statistics(self, product_type, torque):
        # Collect all torques for the product type
        torque_list = list(torque)

        # Calculate summary statistics
        count = len(torque_list)
        mean = np.mean(torque_list)
        std_dev = np.std(torque_list)
        min_val = np.min(torque_list)
        percentiles = np.percentile(torque_list, [25, 50, 75])
        max_val = np.max(torque_list)

        # Yield the summary statistics
        yield (product_type, {
            "count": count,
            "mean": round(mean, 2),
            "std_dev": round(std_dev, 2),
            "min": round(min_val, 2),
            "25th_percentile": round(percentiles[0], 2),
            "50th_percentile": round(percentiles[1], 2),
            "75th_percentile": round(percentiles[2], 2),
            "max": round(max_val, 2)
        })

if __name__ == '__main__':
    MRProductTorqueSummary.run()