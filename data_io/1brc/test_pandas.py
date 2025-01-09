import pandas as pd
import time
from tqdm import tqdm

def process_data(file_path, chunk_size=1000000):
    start_time = time.time()  # Start timing

    # Initialize an empty DataFrame to accumulate results
    accumulated_results = pd.DataFrame()

    # Initialize reader object
    reader = pd.read_csv(file_path, sep=';', header=None, names=['city', 'temp'], chunksize=chunk_size)

    # Process each chunk
    for chunk in tqdm(reader, desc="Processing chunks"):
        # Gr oup by 'city' and calculate min, max, and mean for the chunk
        results = chunk.groupby('city')['temp'].agg(['min', 'max', 'mean']).rename(columns={
            'min': 'temperature_min',
            'max': 'temperature_max',
            'mean': 'temperature_mean'
        })
        # Append chunk results to the accumulated results
        accumulated_results = pd.concat([accumulated_results, results])

    # Final aggregation to ensure city stats are correct across all chunks
    final_results = accumulated_results.groupby('city').agg({
        'temperature_min': 'min',
        'temperature_max': 'max',
        'temperature_mean': 'mean'
    })

    end_time = time.time()  # End timing
    elapsed_time = end_time - start_time  # Calculate elapsed time

    print(f"Elapsed Time: {elapsed_time:.2f} seconds")  # Print the elapsed time
    return final_results

# Specify your file path
file_path = 'measurements.txt'
city_stats = process_data(file_path)
print(city_stats) 
