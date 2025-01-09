import polars as pl

df = pl.scan_csv(
    "measurements.txt", 
    separator=";", 
    has_header=False,
    with_column_names=lambda cols: ["station_name", "measurement"]
)
result = (
    df.select([
        pl.col("station_name"),
        pl.col("measurement")
    ])
    .group_by("station_name")
    .agg([
        pl.col("measurement").quantile(0.25).alias("q25"),
        pl.col("measurement").quantile(0.50).alias("q50"),
        pl.col("measurement").quantile(0.75).alias("q75")
    ])
    .sort("station_name")
    .collect(streaming=True)
)

print("{", end="")
for row in result.iter_rows():
    print(f"{row[0]}={row[1]:.1f}/{row[2]:.1f}/{row[3]:.1f}", end=", ")
print("\b\b} ")