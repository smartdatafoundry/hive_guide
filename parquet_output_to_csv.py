import pandas as pd
import os
from pyarrow import parquet

input_dir = "data-output/test_data"
output_dir = "data-output/test_data_output"

for partition in os.listdir(input_dir):
    if partition.startswith("end_of_this_period="): 
        partition_path = os.path.join(input_dir, partition)
        date_value = partition.split("=")[-1] 
        
        for file in os.listdir(partition_path):
            if file.endswith(".parquet"):
                file_path = os.path.join(partition_path, file)
                df = pd.read_parquet(file_path, engine='pyarrow')
                
                df['end_of_this_period'] = date_value
                
                output_file = os.path.join(output_dir, f"{date_value}.csv")
                df.to_csv(output_file, index=False)
