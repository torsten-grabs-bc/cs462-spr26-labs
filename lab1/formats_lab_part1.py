# Snapshot after Part 3.1 — Write as Parquet and read it back
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pyarrow.compute as pc
import json, os

# 1. Rebuild the Arrow Table (same as Part 2)
with open("/lab/data/sales.json") as f:
    records = json.load(f)
table = pa.Table.from_pylist(records)
revenue = pc.multiply(table["quantity"], table["unit_price"])
table = table.append_column("revenue", revenue)

# 2. Write as Parquet
pq.write_table(table, "/lab/data/sales.parquet")
print("Wrote sales.parquet")

# 3. Read back and confirm schema
loaded_pq = pq.read_table("/lab/data/sales.parquet")
print("\nSchema from sales.parquet:")
print(loaded_pq.schema)
print(f"Rows: {loaded_pq.num_rows}  Columns: {loaded_pq.num_columns}")
