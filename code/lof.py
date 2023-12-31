import pandas as pd
import numpy as np
from pyod.models.lof import LOF
from sklearn.utils import shuffle
import time

# Specify the input file path
input_file = '/home/hadoop/IST3134/preprocessed_data.csv/part-m-00000'

# Load the data and filter out rows with NA values in 'SPEED', 'TRAVEL_TIME', or 'BOROUGH' columns
data_df = pd.read_csv(input_file, na_values=["", "NA", "NaN"])
data_df = data_df.dropna(subset=["SPEED", "TRAVEL_TIME"])

# Take a random sample
sample_size = 200000
data_sample = shuffle(data_df, random_state=123)[:sample_size]

# Run the LOF algorithm on the data sample
data_for_lof = data_sample[["SPEED", "TRAVEL_TIME"]]

# Measure the start time
start_time = time.time()

lof_model = LOF(n_neighbors=5)
lof_model.fit(data_for_lof)
lof_scores = lof_model.decision_scores_

# Measure the end time
end_time = time.time()

# Calculate the elapsed time
execution_time = end_time - start_time
print("Time taken: {:.2f} seconds".format(execution_time))

# Add the LOF scores to the DataFrame
data_sample["lof_score"] = lof_scores

# Specify the output file path
output_file = '/home/hadoop/IST3134/anomalies.csv'

# Write results to CSV file
data_sample.to_csv(output_file, index=False)

# Make a copy of the DataFrame to store category results
data_sample_copy = data_sample.copy()

# Categorize the LOF scores
data_sample_copy["lof_category"] = np.where(np.isnan(lof_scores), "count_na",
                                            np.where(lof_scores > 1, "count_gt1",
                                                     "count_leq1"))

# Specify the output file path
output_file = '/home/hadoop/IST3134/outputpig.csv'
