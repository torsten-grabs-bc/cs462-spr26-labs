# Snapshot after Part 3.4 — Three-way file size comparison
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

# 4. Write as Arrow IPC File Format
with ipc.new_file("/lab/data/sales.arrow", table.schema) as writer:
    writer.write_table(table)
print("\nWrote sales.arrow")

# 5. Read the Arrow IPC file back
with ipc.open_file("/lab/data/sales.arrow") as reader:
    loaded_arrow = reader.read_all()
print("\nSchema from sales.arrow:")
print(loaded_arrow.schema)
print(f"Rows: {loaded_arrow.num_rows}  Columns: {loaded_arrow.num_columns}")

# 6. Column pruning: read only product, region, and revenue
subset = pq.read_table(
    "/lab/data/sales.parquet",
    columns=["product", "region", "revenue"],
)
print("\nColumn-pruned Parquet read (3 of 6 columns):")
print(f"Columns returned: {subset.column_names}")
for i in range(subset.num_rows):
    print(
        f"  {subset['product'][i].as_py():12s} "
        f"{subset['region'][i].as_py():6s} "
        f"${subset['revenue'][i].as_py():8.2f}"
    )

# 7. Three-way file-size comparison
json_size    = os.path.getsize("/lab/data/sales.json")
arrow_size   = os.path.getsize("/lab/data/sales.arrow")
parquet_size = os.path.getsize("/lab/data/sales.parquet")

print("\nFile size comparison:")
print(f"  JSON    : {json_size:>7} bytes")
print(f"  Arrow   : {arrow_size:>7} bytes")
print(f"  Parquet : {parquet_size:>7} bytes")

print(f"\nArrow vs JSON:    {json_size / arrow_size:.2f}x (JSON is larger by this factor)")
print(f"Parquet vs JSON: {json_size / parquet_size:.2f}x (JSON is larger by this factor)")
arrow_vs_pq = arrow_size / parquet_size
rel = "larger" if arrow_vs_pq > 1 else "smaller"
print(f"Arrow vs Parquet: {arrow_vs_pq:.2f}x (Arrow is {rel} than Parquet)")
