import pandas as pd

# Source Data
df_source = pd.DataFrame({
    "ID": [1, 2, 3],
    "NAME": ["A", "B", "C"]
})

# Target Data
df_target = pd.DataFrame({
    "ID": [1, 2, 4],
    "NAME": ["A", "B", "D"]
})


a = df_source.equals(df_target)
print(a)

b = df_source.compare(df_target)
print(b)


# Merge
merged = df_source.merge(
    df_target,
    on="ID",
    how="outer",
    suffixes=("_source", "_target"),
    indicator=True
)

print("Merged Data:")
print(merged)

# Filter common records
common_records = merged[  merged["_merge"]  == "both" ]

print("\nCommon Records:")
print(common_records)
