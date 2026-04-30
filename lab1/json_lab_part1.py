# Snapshot after Part 1.1 — Write the JSON file
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
