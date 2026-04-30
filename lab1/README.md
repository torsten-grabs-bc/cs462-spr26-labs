# Lab 1 — Per-section script snapshots

The Lab 1 handout walks students through four scripts, each built up across multiple sub-sections (e.g., "Append the following block to `json_lab.py` and re-run"). The files here are cumulative snapshots — `partN` contains everything through sub-section `N` — so students can copy the matching snapshot at each step rather than re-typing from the PDF.

| Script | Sub-section | Snapshot |
|---|---|---|
| `json_lab.py`    | 1.1 — write JSON                          | `json_lab_part1.py` |
|                  | 1.2 — read, filter, query                 | `json_lab_part2.py` |
| `arrow_lab.py`   | 2.1 — Arrow Table & schema                | `arrow_lab_part1.py` |
|                  | 2.2 — computed column + filter            | `arrow_lab_part2.py` |
| `formats_lab.py` | 3.1 — Parquet round-trip                  | `formats_lab_part1.py` |
|                  | 3.2 — Arrow IPC round-trip                | `formats_lab_part2.py` |
|                  | 3.3 — column pruning                      | `formats_lab_part3.py` |
|                  | 3.4 — three-way size comparison           | `formats_lab_part4.py` |
| `scale_lab.py`   | 4.1 — generate large dataset              | `scale_lab_part1.py` |
|                  | 4.2 — size comparison at scale            | `scale_lab_part2.py` |
|                  | 4.3 — codec comparison                    | `scale_lab_part3.py` |
|                  | 4.4 — read-time benchmarks                | `scale_lab_part4.py` |
|                  | 4.5 — filtered aggregation                | `scale_lab_part5.py` |

In the container, save the snapshot under the script name the handout uses (e.g., copy `json_lab_part2.py` to `/lab/data/json_lab.py`) so any cross-references in the handout still resolve.
