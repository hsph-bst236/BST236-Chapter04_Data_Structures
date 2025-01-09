import polars as pl
import time
import psutil


def process_data_polars_streaming(file_path):
    # Create a lazy scan of the CSV file
    df = pl.scan_csv(file_path, separator=";", has_header=False, new_columns=["city", "temp"])

    # Define aggregations and group by 'city' with streaming enabled
    results = (
        df.lazy()
        .group_by("city")
        .agg(
            [
                pl.col("temp").min().alias("temperature_min"),
                pl.col("temp").max().alias("temperature_max"),
                pl.col("temp").mean().alias("temperature_mean"),
            ]
        )
        .collect(streaming=True)  # Enable streaming
    )

    return results

# Specify your file path
file_path = 'measurements.txt'
start_time = time.time()
city_stats = process_data_polars_streaming(file_path)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time:.2f} seconds")
print(city_stats) 

process = psutil.Process()
print("Memory usage:", process.memory_info().rss) # print memory usage
print("CPU usage:", process.cpu_percent()) # print CPU usage