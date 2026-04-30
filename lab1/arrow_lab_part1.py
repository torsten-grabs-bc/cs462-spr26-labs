# Snapshot after Part 2.1 — Build an Arrow Table and inspect the schema
import pyarrow as pa
import pyarrow.compute as pc
import json

# 1. Load existing JSON data
with open("/lab/data/sales.json") as f:
    records = json.load(f)

# 2. Build an Arrow Table from the list of dicts
table = pa.Table.from_pylist(records)

# 3. Inspect schema and dimensions
print("Schema:")
print(table.schema)
print(f"\nRows: {table.num_rows}  Columns: {table.num_columns}")
print(f"Column names: {table.column_names}")

# 4. Inspect the unit_price column
unit_prices = table.column("unit_price")
print(f"\nunit_price type  : {unit_prices.type}")
print(f"unit_price values: {unit_prices.to_pylist()}")
