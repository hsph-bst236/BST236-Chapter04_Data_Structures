import pandas as pd
import time
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import os

def process_chunk(chunk):
    """Process a single chunk of data"""
    # Group by 'city' and calculate min, max, and mean for the chunk
    results = chunk.groupby('city')['temp'].agg(['min', 'max', 'mean']).rename(columns={
        'min': 'temperature_min',
        'max': 'temperature_max',
        'mean': 'temperature_mean'
    })
    return results

def process_data(file_path, chunk_size=1000000):    
    # Get the number of CPU cores (leave one core free for system processes)
    num_processes = max(1, cpu_count() - 1)
    print(f"Using {num_processes} processes")

    # Initialize an empty DataFrame to accumulate results
    accumulated_results = pd.DataFrame()

    # Initialize reader object to get total size for progress bar
    total_size = os.path.getsize(file_path)
    total_chunks = total_size // (chunk_size * 100)  # Rough estimate of number of chunks

    # Initialize the process pool
    with Pool(processes=num_processes) as pool:
        # Create a reader that yields chunks
        reader = pd.read_csv(file_path, sep=';', header=None, 
                           names=['city', 'temp'], 
                           chunksize=chunk_size)
        
        # Process chunks in parallel using imap to maintain order
        # Wrap with tqdm for progress tracking
        results = []
        for chunk_result in tqdm(
            pool.imap(process_chunk, reader),
            total=total_chunks,
            desc="Processing chunks"
        ):
            results.append(chunk_result)

        # Combine all results
        accumulated_results = pd.concat(results)

    # Final aggregation to ensure city stats are correct across all chunks
    final_results = accumulated_results.groupby(accumulated_results.index).agg({
        'temperature_min': 'min',
        'temperature_max': 'max',
        'temperature_mean': 'mean'
    })
    return final_results

if __name__ == '__main__':
    # Specify your file path
    file_path = 'measurements.txt'
    start_time = time.time()
    city_stats = process_data(file_path)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(city_stats)
    print(f"Elapsed Time: {elapsed_time:.2f} seconds")
   
