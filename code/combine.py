import pandas as pd

# Read the two CSV files into DataFrames
file1 = '/home/hadoop/IST3134/outputpig.csv'
file2 = '/home/hadoop/IST3134/outputpig1.csv'
data1 = pd.read_csv(file1)
data2 = pd.read_csv(file2)

# Concatenate the two DataFrames to combine their rows
combined_data = pd.concat([data1, data2], ignore_index=True)

# Perform data manipulations and aggregate the result
combined_data["BOROUGH"] = combined_data["BOROUGH"].str.upper()

result = combined_data.groupby("BOROUGH").agg(
    count_leq1=pd.NamedAgg(column="lof_score", aggfunc=lambda x: sum(x <= 1)),
    count_gt1=pd.NamedAgg(column="lof_score", aggfunc=lambda x: sum(x > 1)),
    count_na=pd.NamedAgg(column="has_na", aggfunc="sum")
).reset_index()

# Write the final result to a new CSV file
output_result_file = "/home/hadoop/IST3134/outpig_combined.csv"
result.to_csv(output_result_file, index=False)

print("Combined result saved to:", output_result_file)
