# Snapshot after Part 2.2 — Add a computed column and filter rows
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

# 5. Compute a revenue column: quantity * unit_price
revenue = pc.multiply(table["quantity"], table["unit_price"])
table = table.append_column("revenue", revenue)
print("\nColumn names after adding revenue:", table.column_names)

# 6. Filter to West-region rows only
west_mask = pc.equal(table["region"], "West")
west_table = table.filter(west_mask)
print(f"\nWest-region rows ({west_table.num_rows} records):")
for i in range(west_table.num_rows):
    prod = west_table["product"][i].as_py()
    qty  = west_table["quantity"][i].as_py()
    rev  = west_table["revenue"][i].as_py()
    print(f"  {prod:12s}  qty={qty:4d}  revenue=${rev:8.2f}")

# 7. Total revenue via Arrow compute
total = pc.sum(table["revenue"]).as_py()
print(f"\nTotal revenue (Arrow compute): ${total:,.2f}")
