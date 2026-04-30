# Snapshot after Part 4.1 — Generate a large synthetic dataset
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import json, os, time, random

random.seed(42)

PRODUCTS   = ["Widget A", "Widget B", "Gadget X", "Gadget Y"]
REGIONS    = ["West", "East", "North", "South"]
ROW_COUNTS = [10, 100_000, 1_000_000]


def generate_table(n):
    """Return an Arrow Table with n rows of randomised sales data."""
    ids        = list(range(1, n + 1))
    products   = [random.choice(PRODUCTS) for _ in range(n)]
    quantities = [random.randint(1, 100) for _ in range(n)]
    prices     = [round(random.uniform(5.0, 500.0), 2) for _ in range(n)]
    regions    = [random.choice(REGIONS) for _ in range(n)]

    table = pa.table({
        "id":         ids,
        "product":    products,
        "quantity":   quantities,
        "unit_price": prices,
        "region":     regions,
    })
    revenue = pc.multiply(table["quantity"], table["unit_price"])
    table   = table.append_column("revenue", revenue)
    return table
