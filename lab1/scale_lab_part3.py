# Snapshot after Part 4.3 — Parquet codec comparison
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


# ── 4.2 Size comparison at three scales ──────────────────────────
def write_all_formats(table, n):
    """Write table to JSON, Arrow IPC, and Parquet; return dict of sizes."""
    tag = f"_{n}"
    sizes = {}

    # JSON
    json_path = f"/lab/data/sales{tag}.json"
    rows = table.to_pylist()
    with open(json_path, "w") as f:
        json.dump(rows, f)
    sizes["JSON"] = os.path.getsize(json_path)

    # Arrow IPC
    arrow_path = f"/lab/data/sales{tag}.arrow"
    with pa.OSFile(arrow_path, "wb") as f:
        writer = ipc.new_file(f, table.schema)
        writer.write_table(table)
        writer.close()
    sizes["Arrow IPC"] = os.path.getsize(arrow_path)

    # Parquet (default Snappy compression)
    pq_path = f"/lab/data/sales{tag}.parquet"
    pq.write_table(table, pq_path)
    sizes["Parquet (Snappy)"] = os.path.getsize(pq_path)

    return sizes


print("=" * 65)
print(f"{'Rows':<12} {'JSON':>12} {'Arrow IPC':>12} {'Parquet':>12}")
print("-" * 65)
for n in ROW_COUNTS:
    table = generate_table(n)
    sizes = write_all_formats(table, n)
    print(
        f"{n:<12,} {sizes['JSON']:>12,} "
        f"{sizes['Arrow IPC']:>12,} {sizes['Parquet (Snappy)']:>12,}"
    )
print("=" * 65)


# ── 4.3 Codec comparison ─────────────────────────────────────────
table_1m = generate_table(1_000_000)
codecs = ["NONE", "SNAPPY", "ZSTD"]

print("\nParquet codec comparison (1,000,000 rows):")
print("-" * 45)
print(f"{'Codec':<12} {'Size (bytes)':>14} {'Ratio vs NONE':>14}")
print("-" * 45)

codec_sizes = {}
for codec in codecs:
    path = f"/lab/data/sales_1m_{codec.lower()}.parquet"
    pq.write_table(table_1m, path, compression=codec.lower())
    sz = os.path.getsize(path)
    codec_sizes[codec] = sz

none_size = codec_sizes["NONE"]
for codec in codecs:
    ratio = f"{codec_sizes[codec] / none_size:.2f}x"
    print(f"{codec:<12} {codec_sizes[codec]:>14,} {ratio:>14}")
print("-" * 45)
