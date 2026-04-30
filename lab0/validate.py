import pyarrow as pa
import pandas as pd

# 1. Create a small dataset as a Python list of dicts
records = [
    {"name": "Alice",   "score": 92, "grade": "A"},
    {"name": "Bob",     "score": 78, "grade": "C"},
    {"name": "Charlie", "score": 85, "grade": "B"},
    {"name": "Diana",   "score": 96, "grade": "A"},
    {"name": "Ethan",   "score": 71, "grade": "C"},
]

# 2. Load into a Pandas DataFrame
df = pd.DataFrame(records)
print("=== Pandas DataFrame ===")
print(df)
print(f"\nShape : {df.shape} (rows, columns)")
print(f"dtypes:\n{df.dtypes}")

# 3. Convert the DataFrame to a PyArrow Table
table = pa.Table.from_pandas(df)
print("\n=== PyArrow Table ===")
print(f"Schema : {table.schema}")
print(f"Rows   : {table.num_rows}")
print(f"Columns: {table.num_columns}")

print("\nEnvironment OK — ready for Lab 3.")
