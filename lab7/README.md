# CS462 – Big Data Systems

Lab 7: Media Objects & Spatio-Temporal Data with Spark

Bellevue College  –  Spring 2026

| **Course**         | CS462 – Big Data Systems                                                                  |
| ------------------ | ----------------------------------------------------------------------------------------- |
| **Lab Number**     | Lab 7                                                                                      |
| **Topics**         | Spark UDFs (plain & vectorized), media objects, EXIF, Apache Sedona, spatial joins         |
| **Estimated Time** | 3 hours hands-on + 30 minutes reflection                                                   |
| **Points**         | 120 (+10 bonus available)                                                                  |
| **Prerequisite**   | Lab 6 — same Docker/Spark setup, AWS Learner Lab, and S3 bucket                            |
| **Submission**     | Canvas — upload a single ZIP file (see Submission)                                         |

# Overview

You work with one dataset of **geotagged photographs**, two ways. First as **media objects**: you write UDFs that crack each JPEG open with Pillow to extract features (dimensions, brightness) and the EXIF metadata recording where and when each photo was taken. Then as **spatio-temporal data**: those extracted coordinates and timestamps feed **Apache Sedona** to join photos to Seattle neighborhoods, filter by time of day, and find the photos nearest a landmark.

The lab builds directly on Lab 6 — the image is the Lab 6 Spark image with Pillow and Sedona layered on, reading from the same S3 bucket with the same Requester-Pays config.

## Learning objectives

By the end of this lab you should be able to:

1. Read a collection of media files into Spark as binary using the `binaryFile` data source.
2. Write a plain `@udf` and a vectorized `@pandas_udf` that decode images, and explain the performance difference between them.
3. Extract EXIF metadata inside a UDF, converting GPS coordinates from degrees/minutes/seconds to signed decimal degrees, and handle media that lack the expected metadata.
4. Project binary blobs out of a DataFrame before any shuffle, and identify where in the physical plan that matters.
5. Use Sedona to build geometries, run a spatial join with `ST_Contains` combined with a temporal predicate, and a nearest-neighbor query with `ST_DistanceSphere`.
6. Explain why a spatial join is hard for plain Spark and what a spatial index buys you as data grows.

## Time budget

Plan on **about 3.5 hours**: ~15 min Part 1 (build + verify), ~70 min Part 2, ~30 min Part 3, ~50 min Part 4, ~30 min Part 5.

## AWS cost

No EMR cluster — the only spend is S3 reads of the small (~240-image, <50 MB) dataset, well under a cent. The `content` column holds raw image bytes, so read once, extract features, and work with the small feature columns after that.

---

# Prerequisites

**Items 1–3 are blocking.**

**1. AWS Academy Learner Lab active.** Start the lab, copy the CLI credentials into `~/.aws/credentials` (same as Lab 6), and verify the dataset is visible:

```bash
aws sts get-caller-identity
aws s3 ls --request-payer requester s3://torstengrabs-bc/lab7/ --region us-east-1
```

This must list `images/`, `neighborhoods.csv`, and `landmarks.csv`. `Access Denied` means expired credentials or a missing `--request-payer requester`. (Tokens expire after ~4 hours; refresh and restart the kernel if Spark starts failing mid-lab.)

**2. Docker Desktop running.** Verify with `docker version` and `docker compose version`.

**3. Disk space.** The image is the Lab 6 image plus the Sedona JARs and Pillow — about **4.7 GB**; keep **7 GB free**. Optionally pre-build with `docker compose build`.

**4. Lab 6 completed.** You should be comfortable starting the Dockerized JupyterLab/Spark environment, the S3A SparkSession boilerplate, reading the Spark UI, and the core DataFrame verbs.

---

# Part 1: Setup

**Dataset (staged in S3 under `s3a://torstengrabs-bc/lab7/`):** `images/` (~240 JPEGs across eight Seattle neighborhoods, most with EXIF GPS + time, a few without), `neighborhoods.csv` (polygons as WKT), `landmarks.csv`, and `CREDITS.csv` (photo attribution).

**1.1 Build and start.** From the `lab7/` directory:

```bash
docker compose up --build
```

The first build adds the Sedona/Hadoop JARs and Python libraries on top of the Lab 6 base. When ready, the terminal prints a JupyterLab URL with `token=lab7`; leave it running.

**1.2 Open.** Browse to <http://localhost:8888/?token=lab7> for the two notebooks. The Spark UI comes up at <http://localhost:4040> once a SparkSession starts. (Notebooks appear in a `work/` folder, bind-mounted from the host `lab7/notebooks/`.)

**1.3 Verify S3.** Open `01_media_udfs.ipynb`, run the setup cell and §2.1; a photo count of ~240 means S3A is working.

---

# Part 2: Media objects with UDFs (45 points)

Work through `work/01_media_udfs.ipynb`, §2.1–2.6. Completing this part checks off:

- **LO 1** — Read a collection of media files into Spark as binary using the `binaryFile` data source.
- **LO 3** — Extract EXIF metadata inside a UDF, converting GPS coordinates from degrees/minutes/seconds to signed decimal degrees, and handle media that lack the expected metadata.
- **LO 4** — Project binary blobs out of a DataFrame before any shuffle, and identify where in the physical plan that matters.

**2.1 Read photos as binary (5 pts).** Load `images/` with the `binaryFile` data source; print the schema and count. *(LO 1)*

**2.2 Warm-up: dimensions UDF (given).** Study and run the provided `image_size` UDF — it's the template for the next two. Nothing to submit.

**2.3 Extract GPS + timestamp from EXIF (20 pts).** Write the `extract_geo` UDF. You implement `dms_to_decimal` and the UDF body that reads the GPS and timestamp tags, applies the conversion, and returns `(lat, lon, capture_time)`; photos without GPS must return nulls, not crash. The conversion: *decimal = degrees + minutes/60 + seconds/3600*, negated when the reference is `S` or `W`. *Deliverable: Seattle-range coordinates (lat ≈ 47.6, lon ≈ −122.3) for tagged photos, nulls for untagged. (LO 3)*

**2.4 Mean brightness (10 pts).** Write `mean_brightness`: grayscale histogram → intensity-weighted mean. *(LO 3)*

**2.5 Build the feature frame, drop the blobs (5 pts).** Assemble `path, width, height, brightness, lat, lon, capture_time` into one DataFrame, **dropping `content`**, and write `photo_features.parquet`. Run `.explain()` and confirm the binary column is projected away before any wide operation. *Notebook 2 depends on this file. (LO 4)*

**2.6 Spark UI (5 pts).** Save `notebook1_screenshots/jobs.png` (Jobs tab) and `sql.png` (the plan for the `features.write` query — your UDFs appear as `BatchEvalPython` nodes).

---

# Part 3: Flavors of UDFs (20 points)

Continue in `01_media_udfs.ipynb`, §3.1–3.3. Completing this part checks off:

- **LO 2** — Write a plain `@udf` and a vectorized `@pandas_udf` that decode images, and explain the performance difference between them.

**3.1 Vectorized brightness UDF (12 pts).** Reimplement mean brightness as a `@pandas_udf` over a `pd.Series` of bytes; confirm it matches the plain UDF's values. *(LO 2)*

**3.2 Time both (given).** Run the provided timing cell and note both numbers.

**3.3 Write-up + screenshot (8 pts).** In 1–2 paragraphs: report the timings, explain why the pandas UDF differs (what overhead it removes), and what it does **not** speed up here and why. Save `notebook1_screenshots/udf_compare.png`. *(LO 2)*

---

# Part 4: Spatio-temporal analysis with Sedona (25 points + 10 bonus)

Work through `work/02_sedona_spatiotemporal.ipynb`. It reads the `photo_features.parquet` from Notebook 1, so run Notebook 1 fully first. Completing this part checks off:

- **LO 5** — Use Sedona to build geometries, run a spatial join with `ST_Contains` combined with a temporal predicate, and a nearest-neighbor query with `ST_DistanceSphere`.
- **LO 6** — Explain why a spatial join is hard for plain Spark and what a spatial index buys you as data grows.

**4.1 Build point geometries.** Load the feature frame, drop rows with no GPS, and complete the `ST_Point` call (longitude first, then latitude). *Guided fill-in. (LO 5)*

**4.2 Load neighborhood polygons (given).** Parse the WKT in `neighborhoods.csv` with `ST_GeomFromText`.

**4.3 Daytime photos per neighborhood (12 pts).** Count photos that both fall inside a neighborhood (`ST_Contains`) and were taken between 9:00 and 17:00 (`hour(...)`), grouped and ordered descending. *Expected: 8 rows, 78 photos total. (LO 5)*

**4.4 Five photos nearest the Space Needle (8 pts).** Use `ST_DistanceSphere` and the coordinates from `landmarks.csv`. *Expected: 5 rows, ascending by meters. (LO 5)*

**4.5 Plan inspection & spatial index — bonus (10 pts).** Run `.explain()` on your 4.3 query and answer: (a) which join operator Sedona used (a spatial/range/index join, not a nested-loop all-pairs join), and (b) why a spatial index matters as the point count grows. *(LO 6)*

**4.6 Capstone — brightness × neighborhood (5 pts).** Using the spatial join (no time filter), compute per-neighborhood photo count and average brightness, ordered by brightness descending. Save `notebook2_screenshots/capstone_dag.png` (Stages tab). *Expected: 8 rows, 231 photos total. (LO 4 + LO 5 combined)*

---

# Part 5: Reflection (30 points)

Answer in `reflection.md` (or `.pdf`), 2–3 paragraphs each, citing specific cells/observations.

**5.1 Media vs. tabular (10 pts).** What makes processing media objects in Spark different from tabular data? Using your Notebook 1 work, explain concretely how you kept the binary blobs from being shuffled — point to the step where `content` leaves the pipeline (§2.5) and why doing it there matters. *(LO 4)*

**5.2 UDF vs. UDT, and UDF flavors (10 pts).** What is the difference between a User-Defined Function and a User-Defined Type? Use your UDFs (Notebook 1) and Sedona's geometry type (Notebook 2) as examples. Then, referring to your §3 timings, explain the trade-offs between a plain `@udf` and a `@pandas_udf` and when you'd choose each. *(LO 2)*

**5.3 Why spatial joins need an extension (10 pts).** Why is a spatial join hard for plain Spark when an equi-join is easy? Explain what Sedona adds — both the geometry type and the spatial index/partitioning — referring to the plan from §4.5. What would the join cost without an index, and how does the index change it? *(LO 6)*

---

# Submission

Submit a single zip named `lab7_<yourname>.zip`:

```
lab7_<yourname>.zip
├── 01_media_udfs.ipynb
├── 02_sedona_spatiotemporal.ipynb
├── notebook1_screenshots/   (jobs.png, sql.png, udf_compare.png)
├── notebook2_screenshots/   (capstone_dag.png)
└── reflection.md            (or reflection.pdf)
```

**Do not include:** `docker-compose.yml`, the `Dockerfile`, `.ipynb_checkpoints/`, the Docker image, the image dataset (it's in S3), or `photo_features.parquet`.

## Submission checklist

- [ ] Both notebooks run end-to-end without errors
- [ ] §2.1 prints a photo count of ~240
- [ ] §2.3 returns Seattle-range lat/lon for tagged photos and nulls for untagged
- [ ] §2.5 writes `photo_features.parquet` with no `content` column
- [ ] §3.1 pandas UDF matches the plain UDF; §3.3 write-up present
- [ ] §4.3 produces 8 rows totalling 78 daytime photos
- [ ] §4.4 produces 5 rows, ascending by distance
- [ ] §4.6 produces 8 rows totalling 231 photos
- [ ] Four screenshots present and legible
- [ ] `reflection.md` answers all three questions with cell references
- [ ] Zip named `lab7_<yourname>.zip`

---

# Grading rubric

| **Component**                                   | **Points** | **Criteria**                                                                 |
| ----------------------------------------------- | ---------- | --------------------------------------------------------------------------- |
| 2.1 Read images as binary                       | 5          | `binaryFile` load; schema + count shown                                      |
| 2.3 EXIF GPS + timestamp UDF                    | 20         | Correct DMS→decimal with sign handling; tags read; nulls for untagged        |
| 2.4 Mean brightness UDF                         | 10         | Correct grayscale intensity mean; null-safe                                  |
| 2.5 Feature frame, blobs dropped                | 5          | `content` projected away; `photo_features.parquet` written                   |
| 2.6 Spark UI screenshots                        | 5          | `jobs.png` + `sql.png` present and legible                                   |
| 3.1 Vectorized pandas UDF                       | 12         | `@pandas_udf` over a Series; matches plain-UDF values                        |
| 3.3 Write-up + screenshot                       | 8          | Timings reported; transport vs. decode explained; screenshot present         |
| 4.3 Spatial + temporal join                     | 12         | `ST_Contains` + `hour` filter; 8 rows / 78 photos                            |
| 4.4 Nearest-neighbor                            | 8          | `ST_DistanceSphere`; 5 rows ascending                                        |
| 4.6 Capstone (brightness × neighborhood)        | 5          | Spatial join + `AVG(brightness)`; 8 rows / 231 photos                        |
| 4.5 Plan inspection & spatial index — **bonus** | +10        | Identifies the spatial join operator; explains the index's value            |
| Reflection 5.1 / 5.2 / 5.3                       | 30         | 10 each; correct and grounded in specific cells                              |
| **Total (required)**                            | **120**    |                                                                             |
| **Maximum (with bonus)**                        | **130**    |                                                                             |

> **Note:** The due date is posted on Canvas. Late submissions receive reduced points per the course's late policy.

---

# Appendix A: Useful commands

```bash
docker compose up            # foreground (Ctrl-C to stop)
docker compose up -d         # background
docker compose down          # stop a backgrounded container
docker exec -it lab7-spark-starter bash    # then: ls $SPARK_HOME/jars/ | grep -i sedona
```

To force a fresh session, run `spark.stop()` (or `sedona.stop()`) and re-run the setup cell.

# Appendix B: Troubleshooting

| **Symptom**                                            | **Fix**                                                                                          |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| `Access Denied` reading from S3                        | Refresh Learner Lab creds, update `~/.aws/credentials`, restart the kernel.                       |
| `extract_geo` returns null lat/lon for every photo     | GPS is in `exif.get_ifd(0x8825)` (tags 1–4); timestamp in `exif.get_ifd(0x8769)` (tag 36867).    |
| `ClassNotFoundException` for an `ST_*` function         | Image started without the Sedona jars — `docker compose down`, then `docker compose up --build`.  |
| Notebook 2 can't find `photo_features.parquet`         | Run all of Notebook 1 first; it writes the Parquet into the shared `work/` folder.                |
| `${HOME}` not expanding on Windows                     | Replace `${HOME}/.aws` in `docker-compose.yml` with your full path, e.g. `C:\Users\you\.aws`.     |
| Port 8888/4040 already in use                          | A Lab 6 container is still up — `docker compose down` there, or use the solution image (8889/4041). |

---

*End of lab handout.*
