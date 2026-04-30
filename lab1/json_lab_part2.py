# Snapshot after Part 1.2 — Read, filter, and query the JSON file
import json
import os

# 1. Define a synthetic sales dataset (10 records)
records = [
    {"id": 1,  "product": "Widget A", "quantity": 120, "unit_price": 9.99,  "region": "West"},
    {"id": 2,  "product": "Widget B", "quantity": 85,  "unit_price": 14.49, "region": "East"},
    {"id": 3,  "product": "Gadget X", "quantity": 200, "unit_price": 5.75,  "region": "West"},
    {"id": 4,  "product": "Gadget Y", "quantity": 60,  "unit_price": 24.99, "region": "North"},
    {"id": 5,  "product": "Widget A", "quantity": 95,  "unit_price": 9.99,  "region": "East"},
    {"id": 6,  "product": "Gadget X", "quantity": 150, "unit_price": 5.75,  "region": "South"},
    {"id": 7,  "product": "Widget B", "quantity": 70,  "unit_price": 14.49, "region": "West"},
    {"id": 8,  "product": "Gadget Y", "quantity": 110, "unit_price": 24.99, "region": "East"},
    {"id": 9,  "product": "Widget A", "quantity": 45,  "unit_price": 9.99,  "region": "North"},
    {"id": 10, "product": "Gadget X", "quantity": 180, "unit_price": 5.75,  "region": "South"},
]

# 2. Write to a JSON file
os.makedirs("/lab/data", exist_ok=True)
with open("/lab/data/sales.json", "w") as f:
    json.dump(records, f, indent=2)
print(f"Written {len(records)} records to sales.json")
print(f"File size: {os.path.getsize('/lab/data/sales.json')} bytes")

# 3. Read back and inspect
with open("/lab/data/sales.json", "r") as f:
    loaded = json.load(f)
print(f"\nLoaded {len(loaded)} records")
print("First record:", loaded[0])

# 4. Filter by region
west = [r for r in loaded if r["region"] == "West"]
print(f"\nWest-region records ({len(west)} total):")
for r in west:
    print(f" id={r['id']} product={r['product']} qty={r['quantity']}")

# 5. Compute total revenue
total_revenue = sum(r["quantity"] * r["unit_price"] for r in loaded)
print(f"\nTotal revenue across all records: ${total_revenue:,.2f}")
