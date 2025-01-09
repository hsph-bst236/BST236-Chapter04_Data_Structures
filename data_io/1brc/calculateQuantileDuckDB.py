import duckdb
import time

start_time = time.time()

with duckdb.connect() as conn:
    # Import CSV in memory using DuckDB
    data = conn.sql(
        """
        SELECT
            station_name,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY measurement) AS q25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY measurement) AS q50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY measurement) AS q75
        FROM READ_CSV(
            'measurements.txt',
            header=false,
            columns={'station_name':'VARCHAR','measurement':'DECIMAL(8,1)'},
            delim=';',
            parallel=true
        )
        GROUP BY
            station_name
        """
    )

    # Print final results
    print("{", end="")
    for row in sorted(data.fetchall()):
        print(
            f"{row[0]}={row[1]:.1f}/{row[2]:.1f}/{row[3]:.1f}",
            end=", ",
        )
    print("\b\b} ")

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time:.2f} seconds")