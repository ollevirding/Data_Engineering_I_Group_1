import os
import snappy
import pandas as pd
from pyspark.sql import SparkSession

# Function to deflate data using Snappy compression and write to Parquet format


def deflate_data_to_parquet(source_file, chunk_size, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    chunk_number = 1
    while True:
        chunk = pd.read_csv(source_file, nrows=chunk_size)
        if chunk.empty:
            break

        output_file = os.path.join(output_dir, f'chunk_{chunk_number}.parquet')
        chunk.to_parquet(output_file, compression='snappy')
        chunk_number += 1


# Source path needs to be updated here
source_file = "path/to/your/source/file.csv"
chunk_size = 1000000
output_dir = "path/to/output/directory"  # Output path needs to be  updated.
spark_master = "local[*]"

# deflating data and write to Parquet format
deflate_data_to_parquet(source_file, chunk_size, output_dir)


parquet_files = [os.path.join(output_dir, file) for file in os.listdir(
    output_dir) if file.endswith('.parquet')]
