# CS462 – Big Data Systems

Lab 6: Spark DataFrames over TPC-H on S3

Bellevue College  –  Spring 2026

| **Course**         | CS462 – Big Data Systems                                                                              |
| ------------------ | ----------------------------------------------------------------------------------------------------- |
| **Lab Number**     | Lab 6                                                                                                 |
| **Topics**         | Apache Spark, DataFrames API, SparkSQL, Catalyst optimizer, S3A connector, JupyterLab in Docker       |
| **Estimated Time** | 3 hours hands-on + 30 minutes reflection                                                              |
| **Points**         | 120 (+10 bonus available)                                                                             |
| **Prerequisite**   | Lab 5 — we will re-use the AWS Learner Lab environment and the TPC-H Parquet dataset in S3            |
| **Submission**     | Canvas — upload a single ZIP file (see Deliverables section)                                          |

# Overview

In this lab you will rewrite the Hadoop MapReduce job you wrote in Lab 5 as a Spark DataFrame chain — and then go further with multi-table joins and window functions that would have been impractical in raw MR. You will run everything **locally in Docker** using the official `quay.io/jupyter/pyspark-notebook` image, with PySpark in a JupyterLab notebook. The TPC-H Parquet data still lives in S3 (same bucket as Lab 5), and Spark reads it directly via the S3A connector.

The lab is structured around three notebooks: a DataFrame + Catalyst warm-up, a re-implementation of TPC-H Q1 that you compare line-by-line and plan-node-by-plan-node to your Lab 5 Java code, and a multi-table query with an optional window-function bonus.

## Learning objectives

By the end of this lab you should be able to:

1. Configure a SparkSession that reads Parquet directly from S3 using the S3A connector with Requester-Pays semantics.
2. Use the core DataFrame verbs (`filter`, `select`, `withColumn`, `groupBy`, `agg`, `join`, `orderBy`, `Window`) confidently against a real analytical dataset, and recognize each verb's place in the Catalyst physical plan.
3. Observe predicate pushdown, column pruning, and join-algorithm selection happening in Catalyst's physical plan, and quantify their impact using the Spark UI's `Scan parquet` bytes-read metric.
4. Map a Spark physical plan's `HashAggregate (partial)` → `Exchange` → `HashAggregate (final)` pattern onto the Mapper, Combiner, and Reducer you hand-wrote in Lab 5, and explain why Catalyst's plan is equivalent.
5. Write a multi-table join (4 tables) plus a window function in fewer than 25 lines of PySpark, and reason about why the same query would have required several chained MR jobs.

## Time budget

Plan on **about 3.5 hours** of work spread across the week:

- ~10 minutes for Part 1 (pull the image, start the container, verify S3 access)
- ~45 minutes for Part 2 (Notebook 1 — DataFrame warm-up)
- ~45 minutes for Part 3 (Notebook 2 — TPC-H Q1 in DataFrames and SparkSQL)
- ~60 minutes for Part 4 (Notebook 3 — multi-table join and window function bonus)
- ~30 minutes for Part 5 (reflection write-up)

The first time you start the container, Docker pulls a ~2.5 GB image and Spark resolves ~50 MB of S3A-related JARs from Maven Central. Both are one-time costs; subsequent sessions start in seconds.

## AWS cost budget

This lab is dramatically cheaper than Lab 5 because there is no EMR cluster. The only AWS spend is S3 read traffic from the TPC-H bucket (Requester Pays, billed to your account).

| **Activity** | **Approximate cost** |
|---|---|
| S3 read requests against the SF=1 dataset (each notebook is a few full scans + filtered scans of ~150 MB lineitem) | <$0.10 |
| Cross-region data transfer | $0 (the bucket and Learner Lab both live in `us-east-1`) |
| **Total** | **~$0.10** |

You have effectively unlimited headroom within your $50 Learner Lab budget for this lab. The only real cost watch-out is **don't put a `lineitem.count()` inside a loop while debugging** — each one reads ~150 MB from S3 and costs a couple of cents in egress. Also, **don't run these queries from your laptop against the SF 1000 TPC-H dataset** with 1 TB worth of data.

---

# Prerequisites

Before starting, confirm you have all of the following in working order. **Items 1, 2, and 3 are blocking** — without them you cannot complete the lab.

## 1. AWS Academy Learner Lab is active

Same procedure as Lab 5:

- Log in at https://awsacademy.instructure.com → your CS462 course → **Modules** → **Learner Lab**.
- Click **Start Lab** and wait for the green indicator.
- Click **AWS Details** → under **AWS CLI**, click **Show** and copy the credentials block.
- On your laptop, paste it into `~/.aws/credentials` (the same file you used in Lab 5; replace any expired credentials).

Verify the credentials work:

```bash
aws sts get-caller-identity
aws s3 ls --request-payer requester s3://tpch-torstengrabs-parquet/1GB/ --region us-east-1
```

The second command must return a directory listing including `customer/`, `lineitem/`, `nation/`, `orders/`, and the other TPC-H tables. If you get `Access Denied`, the cause is almost always either missing `--request-payer requester` or expired credentials.

**Note:** Learner Lab session tokens expire after about 4 hours. If your Spark session starts failing partway through Notebook 2, the most likely cause is expired credentials — refresh them in the Learner Lab, paste them into `~/.aws/credentials`, and **restart the Jupyter kernel** (Kernel → Restart) so Spark picks up the new credentials.

## 2. Docker Desktop installed and running

You need **Docker Desktop** running on your laptop for the duration of this lab.

**macOS / Windows:** Install from <https://www.docker.com/products/docker-desktop>. Launch the app; wait until the whale icon in your menu bar / system tray stops animating.

**Linux:** Install `docker-ce` and `docker compose` plugin from your distribution's repositories or from <https://docs.docker.com/engine/install/>. Ensure your user is in the `docker` group (`sudo usermod -aG docker $USER`, then log out and back in).

Verify Docker is running:

```bash
docker version
docker compose version
```

Both commands should print version numbers without errors. If `docker version` complains about a missing daemon, launch Docker Desktop and wait a minute before retrying.

## 3. Disk space and image pre-build

The custom Spark image takes about **4.45 GB on disk** (4.39 GB base + ~60 MB of replacement Hadoop/AWS-SDK JARs). Ensure you have at least **6 GB free** on the disk where Docker stores its data. To pre-build during a time when you have good network, run:

```bash
cd path/to/cs462-spr26-labs/lab6
docker compose build
```

This is optional — `docker compose up --build` will build on demand — but pre-building avoids surprising 5-minute waits during a class lab session.

## 4. Familiarity with prior lab material

This lab assumes you've completed Lab 5 (TPC-H Q1 in Hadoop MapReduce on EMR) and that you can recall the four-component structure of an MR job: Mapper, Combiner, Reducer, and a driver that wires them up. Notebook 2 explicitly maps Spark's physical plan back onto those components, and the reflection asks you to extend that mapping. If Lab 5 is rusty, skim your `Q1Mapper.java`, `Q1Combiner.java`, and `Q1Reducer.java` before starting.

You also need a working understanding of the DataFrame verbs from the Week 07 deck — `filter`, `select`, `withColumn`, `groupBy`, `agg`, `join`, `orderBy`, and `Window` (slides 24–30 of the Week 07 slide deck "DAGs Spark").

---

# Background reading

## From RDDs to DataFrames

You saw RDDs in lecture as the foundational Spark abstraction: a partitioned, lineage-tracked, lazily-evaluated collection of arbitrary Python (or Scala) objects, with transformations like `map`, `filter`, `reduceByKey` and actions like `count`, `collect`. RDDs are powerful but burdensome — every operation is opaque to Spark, so the engine can't reorder, push down, or rewrite anything for you.

DataFrames are a special kind of RDD: every row has the same statically-known schema, and every transformation is expressed through a small surface of well-defined operators (`filter`, `groupBy`, `join`, etc.) instead of arbitrary Python functions. That structure is what lets Spark's Catalyst optimizer reason about your query and rewrite it into a faster equivalent — exactly the same way a SQL query planner does. The cost is a small loss of expressiveness; the benefit, in practice, is large performance gains and substantially shorter code.

In this lab you will write only DataFrames. We don't construct any RDDs by hand. (You can read DataFrames' RDD form via `df.rdd` if you ever need it, but you won't here.)

## Catalyst optimizer recap

Catalyst, Spark's SQL query optimizer, runs on every DataFrame action and SparkSQL query. Its job is to take the *logical* plan (the operators you wrote, in the order you wrote them) and produce a *physical* plan (the actual executable RDD DAG) that is correct and as fast as Catalyst knows how to make it. Three optimizations matter for this lab:

- **Predicate pushdown** moves your `filter` as close to the data source as possible. For Parquet, that means the predicate is evaluated against per-row-group min/max statistics in the file footer, and entire row groups whose range doesn't match the filter are skipped without ever being read. You'll see this in Notebook 2's `Scan parquet` node when you run `.explain()` on Q1, and Notebook 3's section 4.2 asks you to locate it explicitly.
- **Column pruning** reads only the bytes for columns your query actually uses. Parquet's columnar layout makes this very efficient. You'll see this in Notebook 3's section 4.2, where the `Scan parquet` node's `ReadSchema` line lists only the four columns the query actually needs out of `lineitem`'s 16.
- **Join algorithm selection** picks among SortMergeJoin (both sides large), BroadcastHashJoin (one side small enough to fit in memory on every executor), and ShuffleHashJoin (intermediate) based on Catalyst's row-count and size estimates. You'll see this in Notebook 3 when you join `lineitem` to `nation`: Catalyst will broadcast the 25-row `nation` table to every executor instead of shuffling it.

The full Catalyst architecture is described in the Databricks blog post linked from slide 31 of the Week 07 deck; you don't need that level of detail for this lab.

## S3A connector — reading Parquet from S3 in Spark

`s3a://` is the URI scheme of Spark's Hadoop-based S3 client (sometimes called the "S3A FileSystem"). It is implemented by the `hadoop-aws` JAR, which wraps the AWS Java SDK to expose S3 as a Hadoop `FileSystem`. Three details that affect this lab:

1. **Version mismatch handled at image-build time:** Spark 3.5 ships with a Hadoop 3.3.4 client, but the Requester-Pays support you need to read the TPC-H bucket lives in Hadoop 3.3.5+. Mixing the bundled 3.3.4 with a newer `hadoop-aws` at runtime throws `NoClassDefFoundError` (missing `PrefetchingStatistics`). Our `Dockerfile` swaps the bundled `hadoop-client-api`/`hadoop-client-runtime` for 3.3.6 versions and drops `hadoop-aws-3.3.6` + `aws-java-sdk-bundle-1.12.367` into `$SPARK_HOME/jars`. You don't have to manage any of this at SparkSession time.
2. **Credentials:** by default the S3A connector uses `DefaultAWSCredentialsProviderChain`, which walks several credential sources. We pin it explicitly to `ProfileCredentialsProvider`, which reads `~/.aws/credentials` — that's the file `docker-compose.yml` mounts into the container, and the file you maintain via the AWS Academy Learner Lab. Make sure you have your credentials file in that location with the latest credentials from the Learner Lab.
3. **Requester Pays:** because the lab dataset bucket has Requester Pays enabled (you pay for your own reads, billed to your Learner Lab budget), you have to opt in via `spark.hadoop.fs.s3a.requester.pays.enabled=true`. Without that, every read returns `403 Access Denied` even with valid credentials.

You don't need to edit any of this — Notebook 1 has the boilerplate ready — but understanding what each setting does will make troubleshooting much easier when something goes wrong.

---

# Part 1: Setup

## 1.1 Build the image and start the container

The lab uses a small custom Docker image that extends `quay.io/jupyter/pyspark-notebook:spark-3.5.3` with the Hadoop 3.3.6 client JARs (Spark's bundled 3.3.4 client doesn't support Requester Pays). The `Dockerfile` next to `docker-compose.yml` does the swap; `docker compose` will build it on the first launch.

From a terminal, change into the `lab6/` directory of the cloned repo and run:

```bash
cd path/to/cs462-spr26-labs/lab6
docker compose up --build
```

What `--build` does on the **first** launch:

1. Pulls the base `quay.io/jupyter/pyspark-notebook:spark-3.5.3` image (~2.5 GB compressed) — 3–10 minutes depending on bandwidth.
2. Runs the Dockerfile's swap step, downloading ~80 MB of replacement Hadoop + AWS-SDK JARs from Maven Central — about 30 seconds.

After the first build, the result is cached. Subsequent launches can use either `docker compose up` (skip rebuild) or `docker compose up --build` (rebuild if you edited the Dockerfile) — both start in a few seconds.

When the container is ready, the terminal will print something like:

```
jupyter-1  | [I 2026-05-27 10:00:00.000 ServerApp] Jupyter Server 2.x.x is running at:
jupyter-1  | [I 2026-05-27 10:00:00.000 ServerApp] http://...:8888/lab?token=lab6
```

Leave that terminal running for the duration of the lab.

## 1.2 Open JupyterLab and the Spark UI

In a browser, navigate to <http://localhost:8888/?token=lab6>. You should see JupyterLab with the three lab notebooks listed in the file browser.

The Spark UI will be available at <http://localhost:4040> **once you start a SparkSession** (you'll do that in Notebook 1, section 2.1). It is empty until then.

## 1.3 Verify S3 access from PySpark

In the JupyterLab file browser, open the `work/` folder, then open `01_dataframes_and_s3a.ipynb` and run section 2.1 (the SparkSession setup) and section 2.2 (the first `spark.read.parquet`). If `lineitem.printSchema()` prints a 16-column schema, S3A is working. If you see `Access Denied`, your AWS credentials are expired or `requester-pays.enabled` is missing.

> **Note on paths:** The notebooks live on your host machine at `lab6/notebooks/` (inside the cloned repo). The `docker-compose.yml` bind-mounts that folder into the container at `/home/jovyan/work/`, which is where JupyterLab finds them. So in JupyterLab's file browser they appear inside a `work/` folder, even though the host-side folder is named `notebooks/`. Your edits are saved to the host folder either way.

---

# Part 2: DataFrame warm-up (20 points)

Work through **`work/01_dataframes_and_s3a.ipynb`** end to end. The notebook has section headings 2.1 through 2.5 matching this part's structure.

## 2.1 SparkSession configured for S3A

Run the boilerplate cell. Confirm the printed Spark version is 3.5.3.

## 2.2 Load `lineitem` from S3 and count rows

Read the parquet and run `printSchema`, `show(5)`, and `count()` against it. This is the first cell that actually touches S3 — credential / network / Requester-Pays problems surface here.

## 2.3 Exercise — most common ship modes (10 pts)

Write a DataFrame chain that returns the 10 most common values of `l_shipmode` with their counts, sorted descending. Hint: `groupBy` → `count` → `orderBy` → `show`.

## 2.4 Cache and re-run

Cache a filtered+projected DataFrame, run an action twice, and observe the second run reading from in-memory cache instead of S3.

## 2.5 Spark UI tour (10 pts)

Take three screenshots and save them to `notebook1_screenshots/`:

- `jobs.png` — the **Jobs** tab, showing all jobs run in Notebook 1
- `sql.png` — the **SQL / DataFrame** tab for any query above, showing its physical plan
- `storage.png` — the **Storage** tab showing your cached DataFrame

---

# Part 3: TPC-H Q1 — the Lab 5 callback (40 points)

Work through **`work/02_tpch_q1_lab5_callback.ipynb`**. You'll re-implement the same TPC-H Query 1 you wrote as a Hadoop MapReduce job in Lab 5, this time as a PySpark DataFrame chain and as a SparkSQL query. Then you'll read Catalyst's physical plan and find the partial / final HashAggregate split — and you'll see it's structurally identical to the Combiner / Reducer split you wrote in Java.

## 3.1 Q1 in the DataFrame API (10 pts)

Translate the Q1 SQL into a DataFrame chain using `filter`, `groupBy`, `agg` with `F.sum`, `F.avg`, `F.count`, and `orderBy`. Target: about 10 lines of code, producing exactly the 4 rows specified in Lab 5's grading rubric.

## 3.2 Read the physical plan (10 pts)

Call `q1_df.explain(mode="formatted")` and locate each plan node: `Scan parquet`, `Filter`, the partial `HashAggregate`, the `Exchange`, the final `HashAggregate`, the `Sort`. Note which lines on the `Scan parquet` node indicate predicate pushdown and column pruning.

## 3.3 Map plan stages to your Lab 5 code (10 pts)

Fill in the table in the notebook mapping each Lab 5 Java class (`Q1Mapper`, `Q1Combiner`, `Q1Reducer`, the implicit Hadoop shuffle) onto the corresponding Catalyst physical-plan node. Cite the specific node name in your answer.

## 3.4 Q1 in SparkSQL (10 pts)

Register `lineitem` as a SQL view with `createOrReplaceTempView`, then run the same Q1 SQL via `spark.sql(...)`. Show the result.

## 3.5 Compare the two plans (5 pts)

Call `.explain(mode="formatted")` on the SparkSQL result. Compare to 3.2 — the physical plans should be identical. Discuss in the notebook: if the surfaces are equivalent, what factors would push you toward one or the other in production code?

## 3.6 Label narrow vs. wide dependencies on the Q1 DAG (5 pts)

Take a screenshot of the Q1 job's DAG visualization from the Spark UI Stages tab, save as `notebook2_screenshots/q1_dag.png`, and answer the three questions in the notebook (number of stages, location of the stage boundary, which transformations are narrow).

---

# Part 4: Multi-table query & window functions (30 points + 10 bonus)

Work through **`work/03_multi_table_and_windows.ipynb`**. This is the part of the lab that goes meaningfully beyond what you could realistically do in Lab 5 — a four-table join is exactly the case the Week 07 deck cites (slide 5) as motivating Spark's existence.

## 4.1 Revenue per nation (20 pts)

Compute total discounted revenue (`l_extendedprice * (1 - l_discount)`) by customer nation for 1994 shipments. Output one row per nation (25 nations) sorted descending. You'll join `lineitem ⋈ orders ⋈ customer ⋈ nation`.

## 4.2 Read the 4-way join plan (10 pts)

Inspect Catalyst's plan and answer three questions in the notebook: (a) where the date predicate appears, (b) how many columns the `lineitem` scan actually reads (column pruning), and (c) what join algorithm Catalyst picked for `lineitem ⋈ orders`. Justify the algorithm choice in terms of the relative sizes of the inputs.

## 4.3 Top-3 customers per nation — bonus (10 pts)

Extend 4.1 with a window function: identify the three customers with the highest 1994 revenue within each nation. Use `Window.partitionBy("n_name").orderBy(F.desc("cust_revenue"))` and `F.row_number()`, then filter to rank ≤ 3. Expected output: 75 rows (25 nations × 3 customers each).

## 4.4 Plan inspection on the windowed query

Call `.explain(mode="formatted")` on your top-3 query and observe the `Window` operator and the additional `Exchange` it introduces.

## 4.5 Spark UI screenshot

Save the Stages tab screenshot of your top-3 query as `notebook3_screenshots/top3_dag.png`.

---

# Part 5: Reflection (30 points)

Answer the three reflection questions below directly in a file named `reflection.md` (markdown) or `reflection.pdf` (exported from markdown / Word). Each answer should be 2–3 paragraphs.

## 5.1 `F.sum()` vs. Python `sum()` (slide 34, Q1) — 10 pts

Spark provides aggregate functions in `pyspark.sql.functions` (commonly aliased to `F`) that have the same names as Python built-ins — `F.sum`, `F.min`, `F.max`, `F.count`. Python also has built-in `sum`, `min`, `max`, `count`. In your Q1 DataFrame chain you used `F.sum`, not `sum`. What would have happened if you had written `sum("l_quantity")` instead of `F.sum("l_quantity")` inside the `.agg(...)` call? Why? Refer to specific observations from your Notebook 2 work.

## 5.2 DataFrames vs. RDDs for this lab's work (slide 34, Q2) — 10 pts

You have now written every query in this lab as a DataFrame chain. For each of the three notebooks (the warm-up, Q1, the multi-table query), describe what you would have needed to write differently if you had implemented the same query against RDDs instead. Pick one of the three queries and explain whether you would *ever* prefer the RDD implementation for that specific case, citing trade-offs in expressiveness, optimizer support, schema enforcement, and code clarity. Refer to concrete Notebook cells in your answer.

## 5.3 Wide vs. narrow dependencies under node failure (slide 34, Q3) — 10 pts

Imagine that, during the execution of your Notebook 3 top-3 customers query, one of the executor nodes fails after its tasks have finished processing the partial aggregate (Stage 1) but before the final `HashAggregate` (Stage 2) reads its outputs. Describe what Spark does to recover. In your answer, distinguish what happens for partitions whose lineage involves only **narrow** dependencies (such as the parquet scan, filter, and partial aggregate) versus partitions whose lineage involves **wide** dependencies (the shuffle into the final aggregate, and the window's partition-by shuffle). Refer to slide 17 of the Week 07 deck for the dependency taxonomy and to the `.explain()` output of your top-3 query in Notebook 3 (count the `Exchange` nodes to find each wide dependency).

---

# Submission

Submit a single zip file named `lab6_<yourname>.zip` containing:

```
lab6_<yourname>.zip
├── 01_dataframes_and_s3a.ipynb          (your completed Notebook 1)
├── 02_tpch_q1_lab5_callback.ipynb       (your completed Notebook 2)
├── 03_multi_table_and_windows.ipynb     (your completed Notebook 3)
├── notebook1_screenshots/
│   ├── jobs.png
│   ├── sql.png
│   └── storage.png
├── notebook2_screenshots/
│   └── q1_dag.png
├── notebook3_screenshots/
│   └── top3_dag.png
└── reflection.md       (or reflection.pdf)
```

**Do not include:** the `docker-compose.yml`, `.ipynb_checkpoints/`, the Jupyter image, the Parquet data (it's in S3, not your zip), or any cached Spark output.

## Submission checklist

- [ ] All three notebooks run end-to-end against the SF=1 dataset without errors
- [ ] Notebook 1's section 2.3 exercise produces 7 rows of ship-mode counts
- [ ] Notebook 2's Q1 chain produces exactly 4 rows with the canonical `(A,F), (N,F), (N,O), (R,F)` keys
- [ ] Notebook 2's 3.3 table maps each Lab 5 Java class to a specific Catalyst plan node
- [ ] Notebook 3's section 4.1 produces 25 rows (one per nation) sorted by revenue descending
- [ ] Notebook 3's section 4.3 (bonus) produces 75 rows if attempted
- [ ] Six screenshots present and legible
- [ ] Reflection answers all three questions and cites specific cell observations
- [ ] Zip is named `lab6_<yourname>.zip`

---

# Grading rubric

| **Component** | **Points** | **Criteria** |
|---|---|---|
| 2.3 most-common ship modes (Notebook 1) | 10 | Correct DataFrame chain; output is the 7 distinct shipmodes sorted by count |
| 2.5 Spark UI screenshots (Notebook 1) | 10 | Three screenshots present (Jobs, SQL/DataFrame, Storage) and legible |
| 3.1 Q1 DataFrame chain (Notebook 2) | 10 | Filter, groupBy, agg with all 9 aggregates, orderBy; 4 rows with correct keys and counts |
| 3.2 Physical plan reading (Notebook 2) | 10 | Identifies `Scan parquet`, partial `HashAggregate`, `Exchange`, final `HashAggregate`, `Sort` |
| 3.3 Map to Lab 5 classes (Notebook 2) | 10 | Each Java class correctly mapped; the `Exchange` is identified as the shuffle |
| 3.4 Q1 in SparkSQL (Notebook 2) | 5 | View registered; `spark.sql` produces same 4 rows |
| 3.5 Plan comparison (Notebook 2) | 3 | Notes physical plans are identical; reasonable discussion of API vs SQL trade-offs |
| 3.6 Narrow vs. wide labeling (Notebook 2) | 2 | DAG screenshot; correct count of stages; stage boundary identified at `Exchange` |
| 4.1 Revenue per nation (Notebook 3) | 20 | 4-way join correctly composed; filter applied; sum of discounted revenue correct; 25 rows sorted desc |
| 4.2 Plan inspection (Notebook 3) | 10 | All three questions answered with reference to specific plan nodes |
| 4.3 Top-3 customers per nation — **bonus** (Notebook 3) | +10 | Window function correctly defined; 75 rows; ranks 1–3 within each nation |
| Reflection 5.1 `F.sum` vs `sum` | 10 | Identifies the runtime error / wrong behavior; correctly distinguishes Spark Column expressions from Python built-ins |
| Reflection 5.2 DataFrames vs RDDs | 10 | For each notebook, describes the RDD-equivalent work; picks one and argues trade-offs concretely |
| Reflection 5.3 Failure recovery | 10 | Correctly distinguishes narrow-lineage recomputation from wide-lineage recomputation; cites slide 17 |
| **Total (required)** | **120** | |
| **Maximum (with bonus)** | **130** | |

> **Note:** The due date for this lab is posted on Canvas. Submissions received after that date will receive reduced points in accordance with the course's late submission policy.

---

# Appendix A: Useful commands

## Starting and stopping the container

```bash
# From the lab6/ directory, start the container in the foreground (Ctrl-C to stop):
docker compose up

# Or start in the background:
docker compose up -d

# Stop a backgrounded container:
docker compose down
```

## Opening a shell inside the container

Occasionally useful for debugging credentials or Ivy resolution:

```bash
docker exec -it lab6-spark-starter bash
# Inside: ls /home/jovyan/.aws/, ls /home/jovyan/.ivy2/, etc.
```

## Forcing a fresh Spark session

In a notebook cell, before re-creating the SparkSession:

```python
spark.stop()
```

Then re-run the SparkSession.builder cell. The Spark UI tab in your browser may need to be refreshed.

## Watching your S3 spend

```bash
aws ce get-cost-and-usage \
    --time-period Start=$(date -v1d +%Y-%m-%d),End=$(date +%Y-%m-%d) \
    --granularity MONTHLY --metrics UnblendedCost \
    --group-by Type=DIMENSION,Key=SERVICE \
    --query 'ResultsByTime[*].Groups[*].{Service:Keys[0],Cost:Metrics.UnblendedCost.Amount}' \
    --output table
```

---

# Appendix B: Common troubleshooting

| **Symptom** | **Likely cause** | **Fix** |
|---|---|---|
| `Access Denied` from `spark.read.parquet(...)` | Stale AWS credentials, or `requester-pays.enabled` is missing | Refresh creds in Learner Lab, update `~/.aws/credentials`, restart Jupyter kernel. Verify `spark.hadoop.fs.s3a.requester.pays.enabled` is `"true"` in the SparkSession config. |
| `java.lang.ClassNotFoundException: ...S3AFileSystem` or `NoClassDefFoundError: ...PrefetchingStatistics` | The image was started without the Hadoop-3.3.6 swap baked in (e.g., someone replaced `build:` with `image:` in docker-compose.yml) | `docker compose down`, then `docker compose up --build` to rebuild from `Dockerfile`. |
| Port 4040 already in use | Another SparkSession is still alive (often from a prior notebook) | Run `spark.stop()` in the old notebook, or restart all kernels. Or open the Spark UI at port 4041 instead — Spark auto-increments. |
| First S3 read is very slow (~60s) | Cold S3 + Ivy resolution + JVM warmup | Expected on first session of a new container. Subsequent reads should be 5–30s for SF=1 lineitem. |
| `lineitem.show()` works but `.count()` hangs | Usually means S3 connectivity is partially broken (proxy, firewall) | Try `curl https://tpch-torstengrabs-parquet.s3.us-east-1.amazonaws.com/` outside Docker; if that fails, network is the issue, not Spark. |
| `Out of memory` errors during cache | SF=1 should easily fit in 2 GB driver memory; if you've increased SF or run multiple sessions simultaneously, you can hit OOM | Bump `spark.driver.memory` to `"4g"` in the SparkSession config; or stop unused sessions. |
| Jupyter token rejected | Token in URL doesn't match `JUPYTER_TOKEN` env var | Default token is `lab6` — check that's what you typed. If you changed `JUPYTER_TOKEN` in `docker-compose.yml`, restart the container. |
| Container won't start on Apple Silicon | Image is multi-arch so this should not happen | Run `docker pull --platform linux/arm64 quay.io/jupyter/pyspark-notebook:spark-3.5.3` to force a native pull. Unlike Lab 2's apache/hadoop image, this one runs natively on M-series Macs. |

---

*End of lab handout.*
