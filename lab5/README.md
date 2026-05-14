# CS462 – Big Data Systems

Lab 5: TPC-H Analytics with Parquet on Hadoop MapReduce

Bellevue College  –  Spring 2026

| **Course**         | CS462 – Big Data Systems                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------- |
| **Lab Number**     | Lab 5                                                                                              |
| **Topics**         | AWS Academy Learner Lab, Amazon S3, Amazon EMR, Hadoop in the cloud, decoupled storage and compute |
| **Estimated Time** | 3–4 hours (plus AWS provisioning time)                                                             |
| **Points**         | 120                                                                                                |
| **Prerequisite**   | Lab 4 - we will re-use the AWS Learner Lab environment              |
| **Submission**     | Canvas — upload a single ZIP file (see Deliverables section)                                       |

# Overview

In this lab you will explore the columnar storage advantages of Apache Parquet, then implement a real analytical query — TPC-H Query 1 — as a Hadoop MapReduce job that reads Parquet directly. You will measure the impact of compression, predicate pushdown, and the MR combiner pattern on a non-trivial dataset.

The lab uses a 1 GB scale-factor TPC-H dataset for development and a 1 TB scale-factor version for the final measurement run. Both are staged in S3 in `us-east-1`, where your AWS Academy Learner Lab also runs, so you avoid cross-region data transfer fees.

## Learning objectives

By the end of this lab you should be able to:

1. Explain why columnar storage compresses analytical data dramatically better than row-oriented text, and quantify the savings on a real dataset.
2. Describe how Parquet's per-row-group statistics enable predicate pushdown, and explain why an MR job applying its filter inside the mapper does NOT benefit from it.
3. Implement a non-trivial MapReduce program over Parquet input, including using a provided custom `Writable` and writing a combiner.
4. Reason about how a query's wall-time changes when you (a) add a combiner, (b) increase data size, and (c) imagine moving to a vectorized engine or table format.

## Time budget

Plan on **3–4 hours** of work spread across the week:

- ~15 minutes for the size comparison in Part 2 (two `aws s3 ls` commands)
- ~2–3 hours for Part 3 (writing, testing, and running MR code on EMR)
- ~30–45 minutes for the reflection write-up

## AWS cost budget

This lab is designed to fit comfortably within your AWS Academy Learner Lab `$50` allotment. Expected spend with reasonable use:

| **Activity** | **Approximate cost** |
|---|---|
| EMR cluster for all your runs (1 × m5.xlarge primary + 3 × m5.2xlarge core, ~2 hours of active cluster time across the week) | ~$3.50 |
| EMR measurement run on 1 TB dataset (included in the line above) | (included) |
| S3 read requests against the provided datasets | <$0.50 |
| **Total** | **~$4** |

You have headroom for debugging, but **always terminate your EMR cluster when you stop working**. The recommended cluster left running over a weekend would burn ~$60 — more than your entire budget. Set the 1-hour idle auto-termination policy (the launch command below does this) so a forgotten cluster doesn't drain your account overnight.

---

# Prerequisites

Before starting, confirm you have all of the following in working order. **Items 1 and 2 are blocking** — without them you cannot complete the lab.

## 1. AWS Academy Learner Lab is active

- Log in at https://awsacademy.instructure.com → your CS462 course → **Modules** → **Learner Lab**.
- Click **Start Lab** and wait for the green indicator.
- Click **AWS Details** → under **AWS CLI**, click **Show** and copy the credentials block.
- On your laptop, paste it into `~/.aws/credentials` (create the file if missing).

Verify credentials work:

```bash
aws sts get-caller-identity
aws s3 ls --request-payer requester s3://tpch-torstengrabs/1GB/ --region us-east-1
```

The second command must return a directory listing without errors. If you get `Access Denied`, the most common cause is forgetting `--request-payer requester` — the lab dataset has Requester Pays enabled (you pay for your own reads, which is what your $50 budget is for).

## 2. Local Java development environment

You need JDK 17 and Maven on your laptop to build the MR jar. Install using whichever your OS expects; the package contents are the same.

**macOS** (with [Homebrew](https://brew.sh/)):

```bash
brew install --cask corretto@17
brew install maven
```

**Windows** (PowerShell, with `winget` preinstalled on Windows 10/11):

```powershell
winget install --id Amazon.Corretto.17.JDK
winget install --id Apache.Maven
```

Open a fresh PowerShell window afterwards so the updated `PATH` is picked up. If `winget` isn't available on your machine, an alternative is to download the Corretto 17 `.msi` installer from <https://docs.aws.amazon.com/corretto/latest/corretto-17-ug/downloads-list.html> and Maven from <https://maven.apache.org/download.cgi> (unzip, add the `bin/` directory to `PATH`).

**Linux — Debian/Ubuntu:**

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk maven
```

**Linux — Fedora/RHEL/CentOS Stream:**

```bash
sudo dnf install -y java-17-openjdk-devel maven
```

**Verify on any OS:**

```bash
java -version   # Should print 17.x.x (Corretto-17.x.x.x or OpenJDK 17.x.x)
mvn -version    # Should print Apache Maven 3.x with Java version: 17.x.x
```

If `mvn -version` reports a Java version other than 17, set `JAVA_HOME` to point at your JDK 17 install:

- macOS: `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`
- Linux: `export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))`
- Windows: System Properties → Advanced → Environment Variables → add `JAVA_HOME` pointing at your Corretto install dir (typically `C:\Program Files\Amazon Corretto\jdk17.x.x_x`)

## 3. VS Code with the Extension Pack for Java (recommended)

Not required, but makes Part 3 much smoother. Install **Extension Pack for Java** by Microsoft from the Extensions sidebar; it'll auto-detect the project's `pom.xml`.

## 4. Familiarity with prior lab material

This lab assumes you've completed Lab 3 (intro to Hadoop MR with word count) and Lab 4 (EMR on AWS Academy Learner Lab). If either of those is rusty, re-read the lab handouts before starting Part 3.

---

# Background reading

## What is TPC-H?

TPC-H is an industry-standard benchmark for decision-support workloads — the kind of analytical queries you'd find in a business intelligence dashboard or data warehouse. It defines a fixed schema (8 tables modeling a parts-supplier business) and 22 standard queries. Database vendors publish TPC-H results to benchmark their systems against competitors.

The schema is small enough to fit in your head:

```
region (5 rows) ───────┐
                       │
nation (25 rows) ──────┤
                       │
supplier ──────────────┤
   │                   │
   └──── partsupp ──── part
              │
              └──── lineitem ──── orders ──── customer
```

For this lab, you'll focus on **lineitem**, the biggest table (~6 million rows at SF=1, ~6 billion at SF=1000). It records every line item on every order — quantities, prices, discounts, ship dates. It looks like:

| **Column** | **Type** | **Example** |
|---|---|---|
| l_orderkey | bigint | 1 |
| l_partkey | bigint | 155190 |
| l_suppkey | bigint | 7706 |
| l_linenumber | int | 1 |
| l_quantity | double | 17.0 |
| l_extendedprice | double | 21168.23 |
| l_discount | double | 0.04 |
| l_tax | double | 0.02 |
| l_returnflag | string | "N" |
| l_linestatus | string | "O" |
| l_shipdate | string | "1996-03-13" |
| l_commitdate | string | "1996-02-12" |
| l_receiptdate | string | "1996-03-22" |
| l_shipinstruct | string | "DELIVER IN PERSON" |
| l_shipmode | string | "TRUCK" |
| l_comment | string | "egular courts above the" |

The full schema for all 8 tables is documented at https://www.tpc.org/tpch/.

## What is Parquet?

Parquet is a binary, columnar, compressed file format optimized for analytics. Three things matter:

1. **Columnar layout.** Within each row group (a horizontal chunk of the data), all values of column A are stored together, then all values of column B, and so on. This means a query that only needs three columns out of sixteen reads roughly 3/16 of the bytes from disk.
2. **Compression and encoding.** Each column chunk is compressed (Snappy in our dataset) and often encoded with techniques like dictionary encoding for low-cardinality strings (`l_returnflag` has 3 distinct values across 6 billion rows — Parquet stores those 3 values once and uses tiny integers as references).
3. **Statistics in the footer.** Each row group records min/max values per column. A query like `WHERE l_shipdate >= '1998-01-01'` can skip entire row groups whose `l_shipdate` max is older — without reading the data. This is called **predicate pushdown**.

The same dataset stored as pipe-separated text vs. Snappy-compressed Parquet typically shows a 3–5× compression ratio plus a 5–20× scan-time improvement when the query touches only a few columns. You're going to measure both.

## What is TPC-H Query 1?

TPC-H Q1 is the canonical "easy" benchmark query: a single-table scan with filtering, grouping, and aggregation. Here's the SQL:

```sql
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity)                                 AS sum_qty,
    SUM(l_extendedprice)                            AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount))         AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity)                                 AS avg_qty,
    AVG(l_extendedprice)                            AS avg_price,
    AVG(l_discount)                                 AS avg_disc,
    COUNT(*)                                        AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

In plain English: for every (returnflag, linestatus) combination, compute eight aggregates over shipped lineitems. The output is exactly 4 rows (the four combinations that exist in TPC-H data).

In MR terms, this maps cleanly onto:

- **Mapper**: read each lineitem row from Parquet; if `l_shipdate <= '1998-09-02'`, emit `(returnflag, linestatus) → (qty, ext_price, disc_price, charge, disc, count_one)`
- **Combiner**: locally sum partial aggregates from the same mapper, dramatically cutting the data shuffled to reducers
- **Reducer**: sum across all combiners' partial results, divide sums by count to get averages, emit one output row per group

You'll implement all three.

---

# Part 1: Setup

The provided datasets live in `us-east-1` with **Requester Pays** enabled. You pay for your reads (out of your Learner Lab budget), the instructor pays for storage.

| **Dataset** | **S3 location** | **Approximate size** |
|---|---|---|
| Raw text (1 GB scale) | `s3://tpch-torstengrabs/1GB/` | ~1 GB |
| Raw text (1 TB scale) | `s3://tpch-torstengrabs/1TB/` | ~1 TB |
| Parquet (1 GB scale) | `s3://tpch-torstengrabs-parquet/1GB/` | ~300 MB |
| Parquet (1 TB scale) | `s3://tpch-torstengrabs-parquet/1TB/` | ~300 GB |

**Every CLI command that reads these buckets needs `--request-payer requester`** or you'll get `Access Denied`.

```bash
# Verify your access
aws s3 ls --request-payer requester s3://tpch-torstengrabs-parquet/1GB/lineitem/ --region us-east-1
```

You should see a few `.parquet` files listed with their sizes.

---

# Part 2: Quantify Parquet's storage advantage

Before writing any MR code, you'll measure how much smaller the same data is in Parquet. No cluster or extra tooling needed — just `aws s3 ls`.

## 2.1 Compare raw size vs Parquet size

Two `aws s3 ls --summarize` commands tell you the total bytes for `lineitem` in text vs Parquet at the 1 GB scale:

```bash
# Text size
aws s3 ls --recursive --summarize \
    --request-payer requester --region us-east-1 \
    s3://tpch-torstengrabs/1GB/lineitem/ | tail -2

# Parquet size
aws s3 ls --recursive --summarize \
    --request-payer requester --region us-east-1 \
    s3://tpch-torstengrabs-parquet/1GB/lineitem/ | tail -2
```

**Record:** the two byte totals and the compression ratio (text bytes ÷ Parquet bytes).

Optionally, repeat for the 1 TB scale (`s3://tpch-torstengrabs/1TB/lineitem/` and `s3://tpch-torstengrabs-parquet/1TB/lineitem/`) and confirm the ratio is similar — Parquet's compression is scale-independent.

You'll cite these numbers in the reflection.

---

# Part 3: Implement TPC-H Q1 in Hadoop MapReduce (90 points)

You'll fill in three Java files: `Q1Mapper`, `Q1Combiner`, and `Q1Reducer`. The project skeleton (Maven project, schemas, driver class, build config, and a complete `Q1Value` Writable) is provided. Look for `// TODO:` markers — those are the places you need to write code.

## Get the skeleton

The starter project lives in the course repo under `lab5/`:

```bash
mkdir -p ~/cs462
cd ~/cs462
git clone https://github.com/torsten-grabs-bc/cs462-spr26-labs.git
cd cs462-spr26-labs/lab5
ls
```

You should see:

```
README.md                                  ← this handout
pom.xml
.gitignore
src/main/resources/lineitem.avsc                  ← full schema (reference)
src/main/resources/lineitem-q1-projection.avsc    ← Q1's column subset
src/main/java/lab5/Q1Driver.java                  ← complete; read-only for you
src/main/java/lab5/Q1Value.java                   ← complete; read-only for you
src/main/java/lab5/Q1Mapper.java                  ← TODO
src/main/java/lab5/Q1Combiner.java                ← TODO
src/main/java/lab5/Q1Reducer.java                 ← TODO
```

You'll fill in the three files marked `TODO`. Do not modify `Q1Driver.java`,
`Q1Value.java`, the schemas, or `pom.xml` — they're configured to wire
everything together correctly. If you have changes you think are needed,
ask on the course forum first.

## Architecture

The data flow:

```
Parquet files in S3
       ↓
AvroParquetInputFormat → GenericRecord per row
       ↓
Q1Mapper:  filter on l_shipdate; emit (returnflag,linestatus) → Q1Value(partial sums)
       ↓
Q1Combiner: same-key partial sums collapse locally → Q1Value
       ↓
   network shuffle
       ↓
Q1Reducer: final sum across all combiners; emit 4 rows of CSV
       ↓
TextOutputFormat → S3
```

## About `Q1Value` (provided complete)

`Q1Value` is a custom `Writable` that holds the six numeric values that flow between mapper, combiner, and reducer:

| **Field** | **Type** |
|---|---|
| sumQty | double |
| sumBasePrice | double |
| sumDiscPrice | double |
| sumCharge | double |
| sumDisc | double |
| count | long |

You don't need to modify it — open the file once and skim through, particularly `write()`, `readFields()`, and `add()`. You'll call `add()` from your combiner and reducer, and `set()`/`clear()` from your mapper.

## 3.1 Implement `Q1Mapper`

Extends `Mapper<Void, GenericRecord, Text, Q1Value>`. (Avro Parquet sends the GenericRecord as the value; key is unused.)

**Fill in:**

1. Read these fields from the input record:
   - `l_shipdate` (CharSequence — Avro gives you a `Utf8`; convert with `.toString()`)
   - `l_returnflag`, `l_linestatus` (same)
   - `l_quantity`, `l_extendedprice`, `l_discount`, `l_tax` (doubles)
2. If `l_shipdate.compareTo("1998-09-02") > 0`, skip this record.
3. Compute the four partial sums for this single row:
   - `extPrice * (1 - discount)` — disc_price
   - `extPrice * (1 - discount) * (1 + tax)` — charge
4. Emit `(returnflag + "|" + linestatus, Q1Value(qty, extPrice, disc_price, charge, discount, 1L))`.

A small efficiency note: allocate the output `Text` and `Q1Value` objects once as instance fields and reuse them across `map()` calls. Hadoop maps process millions of records — allocating new objects per call thrashes the GC.

## 3.2 Implement `Q1Combiner`

Extends `Reducer<Text, Q1Value, Text, Q1Value>` — yes, combiners use the Reducer base class.

**Fill in:** for each key, sum all incoming `Q1Value`s using `Q1Value.add()` and emit the single combined result.

## 3.3 Implement `Q1Reducer`

Extends `Reducer<Text, Q1Value, Text, Text>`. Note the output is `Text` rather than `Q1Value` because we want a human-readable CSV row.

**Fill in:** for each key:

1. Sum all incoming `Q1Value`s.
2. Compute the three averages: `avg_qty = sum_qty / count`, `avg_price = sum_base_price / count`, `avg_disc = sum_disc / count`.
3. Format and emit a CSV row:
   ```
   returnflag|linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count
   ```

## 3.4 Build

```bash
cd ~/cs462/lab5/lab5_q1
mvn -DskipTests package
ls -lh target/*.jar
```

You should see a shaded fat jar at `target/lab5-q1-job.jar` (~30 MB).

## 3.5 Upload jar to S3 and launch an EMR cluster

In your Learner Lab, create a workspace bucket for your output and jar:

```bash
# Generate a unique-ish suffix to avoid collisions
SUFFIX=$(whoami)-$(date +%s)
MY_BUCKET=cs462-lab5-$SUFFIX
echo "Your bucket: $MY_BUCKET"

aws s3api create-bucket --bucket $MY_BUCKET --region us-east-1
aws s3 cp target/lab5-q1-job.jar s3://$MY_BUCKET/jars/
```

Launch an EMR cluster (~10 minutes to be ready):

```bash
SUBNET=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true \
    --query 'Subnets[0].SubnetId' --output text --region us-east-1)

aws emr create-cluster --region us-east-1 \
  --name "cs462-lab5-$USER" \
  --release-label emr-7.12.0 \
  --applications Name=Hadoop \
  --service-role EMR_DefaultRole \
  --ec2-attributes "{\"InstanceProfile\":\"EMR_EC2_DefaultRole\",\"SubnetId\":\"$SUBNET\"}" \
  --instance-groups \
    'InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1' \
    'InstanceGroupType=CORE,InstanceType=m5.2xlarge,InstanceCount=3' \
  --log-uri s3://$MY_BUCKET/emr-logs/ \
  --auto-termination-policy IdleTimeout=3600 \
  --configurations '[{"Classification":"core-site","Properties":{"fs.s3.useRequesterPaysHeader":"true","fs.s3a.requester.pays.enabled":"true"}}]'
```

The `EMR_DefaultRole` and `EMR_EC2_DefaultRole` IAM names are the standard EMR
roles. Your Learner Lab account should already have them. If the cluster create
fails with a role-not-found error, verify the names that actually exist with:

```bash
aws iam list-roles --query 'Roles[?starts_with(RoleName, `EMR`)].RoleName' --output table
```

and substitute whatever names show up.

Note also the `--configurations` flag setting `fs.s3.useRequesterPaysHeader=true` — without this, the cluster cannot read the Requester Pays datasets.

Save the cluster ID it returns. Wait for it to reach `WAITING` state:

```bash
CLUSTER=<the-cluster-id>
watch -n 30 "aws emr describe-cluster --region us-east-1 --cluster-id $CLUSTER \
    --query 'Cluster.Status.State' --output text"
```

## 3.6 Submit your Q1 job against the 1 GB dataset

```bash
JAR=s3://$MY_BUCKET/jars/lab5-q1-job.jar
INPUT=s3://tpch-torstengrabs-parquet/1GB/lineitem/
OUTPUT=s3://$MY_BUCKET/output/q1-1gb/

aws emr add-steps --region us-east-1 --cluster-id $CLUSTER --steps "Type=CUSTOM_JAR,\
Name=q1-1gb,\
ActionOnFailure=CONTINUE,\
Jar=$JAR,\
Args=[$INPUT,$OUTPUT]"
```

Watch the step:

```bash
aws emr list-steps --region us-east-1 --cluster-id $CLUSTER --output table
```

When the step finishes (`COMPLETED`), inspect the output:

```bash
aws s3 ls s3://$MY_BUCKET/output/q1-1gb/
aws s3 cp s3://$MY_BUCKET/output/q1-1gb/part-r-00000 - | sort
```

You should see exactly **four rows**, one per `(l_returnflag, l_linestatus)` combination that exists in TPC-H data:

| **returnflag\|linestatus** | **what it means** |
|---|---|
| `A\|F` | returned, finalized |
| `N\|F` | not returned, finalized |
| `N\|O` | not returned, still open |
| `R\|F` | refunded, finalized |

Each output line is tab-separated `key<TAB>value`, where the `value` is 8 comma-separated numbers in this exact order:

```
sum_qty , sum_base_price , sum_disc_price , sum_charge , avg_qty , avg_price , avg_disc , count_order
```

For the SF=1 (1 GB) dataset, the canonical TPC-H Q1 reference output is below. A truncated set of columns is shown so the table fits on the page; your implementation must produce **all eight value columns** (`sum_qty`, `sum_base_price`, `sum_disc_price`, `sum_charge`, `avg_qty`, `avg_price`, `avg_disc`, `count_order`) and match the reference exactly on each one.

| **group** | **sum_qty** | **sum_base_price** | **sum_disc_price** | **…** | **count_order** |
|---|---:|---:|---:|:---:|---:|
| `A\|F` | 37,734,107.00 | 56,586,554,400.73 | 53,758,257,134.87 | … | 1,478,493 |
| `N\|F` | 991,417.00 | 1,487,504,710.38 | 1,413,082,168.05 | … | 38,854 |
| `N\|O` | 74,476,040.00 | 111,701,729,697.74 | 106,118,230,307.61 | … | 2,920,374 |
| `R\|F` | 37,719,753.00 | 56,568,041,380.90 | 53,741,292,684.60 | … | 1,478,870 |

Quick reading of the A|F row: 1,478,493 returned-and-finalized lineitems together shipped 37.7M units worth **$56.6B at list price**; after an average 5% discount customers paid **$53.8B**, and after taxes the seller netted **$55.9B**.

If your output has more or fewer than 4 rows, wrong keys, or counts/sums that disagree with the reference, debug your filter and aggregate math in `Q1Mapper` and `Q1Reducer`. (Small rounding differences in the last decimal place on averages are fine.)

**Take a screenshot** of:
- The EMR Steps tab showing your step as `Completed`
- The terminal output of `aws s3 cp …/part-r-00000 -` showing the four rows

## 3.7 Measurement run on the 1 TB dataset

Same job, larger input. Use a fresh output prefix:

```bash
INPUT_1TB=s3://tpch-torstengrabs-parquet/1TB/lineitem/
OUTPUT_1TB=s3://$MY_BUCKET/output/q1-1tb/

aws emr add-steps --region us-east-1 --cluster-id $CLUSTER --steps "Type=CUSTOM_JAR,\
Name=q1-1tb,\
ActionOnFailure=CONTINUE,\
Jar=$JAR,\
Args=[$INPUT_1TB,$OUTPUT_1TB]"
```

This will take **~35–45 minutes** on the recommended cluster (1 × m5.xlarge primary + 3 × m5.2xlarge core) — most of it spent reading the (column-projected) Parquet data from S3 and running 1,599 mapper tasks. The 1 TB dataset is stored as 1,599 Parquet files (~188 MB each), each becoming one input split, so a cluster with more concurrent mapper containers finishes proportionally faster. If you launched a smaller cluster (e.g., 2 × m5.xlarge), expect this step to take 60–100 minutes instead.

Once it completes, capture two things:

**(a) Step duration.** EMR console → click your cluster → **Steps** tab. Take a screenshot of the row showing the 1 TB step with its `Elapsed` / `Duration` column.

**(b) MR counters from the syslog.** EMR doesn't show counters in the console as a dedicated tab — they live inside the step's `syslog` log file in S3. Pull it down and extract the counter summary at the end:

```bash
STEP_ID=<the s-XXXXX id of your 1 TB step from the Steps tab>
aws s3 cp s3://$MY_BUCKET/emr-logs/$CLUSTER/steps/$STEP_ID/syslog.gz - \
    | gunzip | grep -A 60 "Counters:"
```

That dumps the full counter summary. Take a screenshot of (or copy into your reflection) at least these counters, all from the `Map-Reduce Framework` and `File System Counters` groups:

- `Map input records` — should be ~6 billion at SF=1000
- `Map output records` — rows emitted by the mapper (after filter, before combine)
- `Combine input records` / `Combine output records` — ratio shows the combiner's effectiveness
- `Reduce input records` — how much actually crossed the network to the reducer
- `Reduce shuffle bytes` — total bytes shuffled
- `S3A_BYTES_READ` (or `S3_BYTES_READ` depending on EMR release) — Parquet bytes pulled from S3

If you'd rather click around than CLI, the same syslog is reachable from EMR console → cluster → Steps → click the step → **View logs** link → drill into the step's log folder → open `syslog` (or `syslog.gz`). The "Counters:" section is at the very bottom of the file.

## 3.8 Terminate your cluster

When you're done with all measurements:

```bash
aws emr terminate-clusters --region us-east-1 --cluster-ids $CLUSTER
```

Verify it's terminating:

```bash
aws emr list-clusters --region us-east-1 --active --output table
```

This cluster should not appear in the active list.

---

# Preview: where this leads next week

You now have a working MR pipeline that scanned 1 TB of `lineitem`, ran TPC-H Q1 through your mapper/combiner/reducer, and produced the canonical 4-row result.

Next week's lab keeps the same query and the same Parquet data, and changes two things:

1. **Replace MapReduce with Apache Spark** as the execution engine.
2. **Register the same Parquet files as an Apache Iceberg table** — a "table format" layer that sits on top of Parquet and adds metadata about how data is partitioned and organized.

Hold on to the wall time, scan-bytes, and counter values you captured this week. They are your baseline. Next week you'll measure how each of those two changes affects them — with your own eyes, on your own cluster.

Reflection Question 4.5 below asks you to **predict** how those changes might affect performance, and reason about why, before you measure anything next week.

---

# Part 4: Reflection (30 points)

Answer the following five questions in 1–2 paragraphs each. Cite the specific numbers you recorded in Parts 2 and 3. Submit as `reflection.pdf` or `reflection.docx`.

## 4.1 Per-column compression observations

Look at the 16 columns in the `lineitem` schema (listed in the Background Reading section). Based on column cardinality (number of distinct values), value distribution, and what you know about Parquet's encoding strategies (dictionary, run-length encoding / RLE, plain):

- Predict the **three columns you'd expect to compress best** (highest compressed-to-uncompressed ratio) and explain why for each.
- Predict the **three columns you'd expect to compress worst**, and explain why for each.

For your "best" picks especially, name the specific encoding (dictionary vs. RLE vs. delta) you'd expect Parquet to choose and why it fits.

## 4.2 Column projection vs. predicate pushdown

In your Q1 implementation, the filter `l_shipdate <= '1998-09-02'` runs inside `Q1Mapper.map()` — meaning every row is read from Parquet and *then* thrown away if its shipdate is past the cutoff.

(a) **Predicate pushdown** at the Parquet reader level skips entire row groups without reading them. Explain what Parquet stores in each row group's footer that makes this possible, and how a query engine would use it to decide which row groups to skip when evaluating `l_shipdate <= '1998-09-02'`.

(b) Why doesn't your MR job benefit from predicate pushdown the way a query engine like Spark or DuckDB would? Specifically: at what level in the stack is the filter applied in your code, and what would have to change for the Parquet reader itself to evaluate it?

(c) Your driver *does* use one form of "pushdown" — **column projection** via `setRequestedProjection`. Given that `lineitem` has 16 columns and Q1's projection schema lists 7, estimate roughly what fraction of the bytes per row the Parquet reader actually transferred. (Be careful: Parquet stores columns separately, but they aren't all the same size — the date and numeric columns are smaller than the comment column.)

## 4.3 Predicate selectivity and where the filter actually runs

From your 1 TB counter dump in Exercise 3.7, two counters tell you how selective the Q1 predicate is on this dataset:

- `Map input records` — total lineitem rows read from Parquet (before any filter)
- `Map output records` — rows your mapper emitted (i.e., rows that passed `l_shipdate <= '1998-09-02'`)

(a) Compute the **selectivity of the current Q1 predicate** as `Map output records / Map input records`. Report both raw counts and the resulting percentage. (Hint: it's surprisingly high — Q1's filter excludes only a small fraction of the data.)

(b) **What-if scenario:** imagine you rewrote the WHERE clause from `l_shipdate <= '1998-09-02'` to `l_shipdate >= '1998-06-01'` — keeping only the last ~3 months of data instead of ~7 years' worth. That's roughly **27× more selective** than Q1's default.

If you re-ran the same MR job with that new predicate on the same 1 TB Parquet dataset, what would you expect to happen to each of these metrics? For each, predict **substantially less / slightly less / about the same**, and justify in one sentence:

- Wall time
- `S3A_BYTES_READ` (bytes the cluster fetched from S3)
- `Map output records` (rows your mapper emitted after filtering)
- `Combine output records` (rows that crossed the network to the reducer)

(c) Connect your predictions to Reflection Q4.2: **which layer of the stack would have to change** for the more selective predicate to actually deliver a substantial wall-time speedup, and why?

## 4.4 Combiner impact

From your 1 TB counter dump in Exercise 3.7, find these three counters:

- `Map output records` — how many partial aggregates your mappers emitted
- `Combine input records` — how many of those the combiner processed locally (before shuffle)
- `Combine output records` — how many records actually crossed the network to the reducer

The ratio `Combine input records / Combine output records` is the combiner's compression factor — for every N partial aggregates that went in, only 1 came out. Report the three numbers and compute the ratio.

Then explain:

(a) Why the ratio is so high for Q1 specifically. What mathematical property of the per-row aggregates (`sum_qty`, `sum_extprice`, etc.) lets the combiner collapse many partials into one without changing the final result? (Hint: think about which operations are *associative* and *commutative*.)

(b) Identify a hypothetical query — over the same `lineitem` table — where adding a combiner would help much less, or not at all. Briefly justify why a combiner doesn't reduce the work in that case.

## 4.5 Forward-looking — Spark and Iceberg

Next week's lab keeps the same Q1 query and the same Parquet data, but introduces two changes:

1. The execution engine changes from **Hadoop MapReduce to Apache Spark** (running over the same Parquet files in S3).
2. The same Parquet files get registered as an **Apache Iceberg table**, partitioned by `year(l_shipdate)`.

For each change, write a short prediction (1–2 paragraphs each) about how you think it will affect performance, and why. Use your this-week numbers — wall time, `S3A_BYTES_READ`, `Combine input/output records`, mapper count — as your baseline.

(a) **MapReduce → Spark over the same Parquet.** Do you expect wall time to go up, down, or stay roughly the same? Do you expect the bytes read from S3 to change? Reason about *why* — what does a modern analytics engine like Spark do differently from Hadoop MR that might matter for a query like Q1? (You don't need detailed Spark knowledge yet; an architectural-level guess is what we're after.)

(b) **Plain Parquet → Iceberg-partitioned Parquet (still on Spark).** TPC-H `lineitem` ship dates span Jan 1992 through Dec 1998 — seven years. Q1's filter is `l_shipdate <= '1998-09-02'`. If Iceberg stores per-partition metadata about the min/max `l_shipdate` in each year-partition, how could a query planner use that to do less work than even Spark-over-plain-Parquet would do? Roughly quantify your expectation: what fraction of the data do you think Spark would actually have to read, and why?

Write down your predictions before next week. After you measure the real numbers, you'll be able to look back and see which parts of your intuition were right and which weren't — that's where the learning lives.

---

# Submission

Submit a single zip file named `lab5_<yourname>.zip` containing:

```
lab5_<yourname>.zip
├── Q1Mapper.java
├── Q1Combiner.java
├── Q1Reducer.java
├── screenshots/
│   ├── step-1gb-completed.png
│   ├── step-1gb-output-rows.png
│   ├── step-1tb-counters.png
│   └── cluster-terminated.png
└── reflection.pdf    (or reflection.docx)
```

**Do not include:** `target/`, `.idea/`, `pom.xml`, `Q1Driver.java`, `Q1Value.java` (provided to you, you didn't write it), the lineitem Parquet data, or your jar file. Only the three files you wrote, the screenshots, and your reflection.

## Submission checklist

- [ ] All three Java files compile and your output on 1 GB has the expected 4 rows with correct keys and approximately-correct row counts
- [ ] Four screenshots present and legible
- [ ] Reflection answers all five questions and cites specific numerical observations
- [ ] EMR cluster terminated (verify with `aws emr list-clusters --active`)
- [ ] Zip is named correctly (`lab5_<yourname>.zip`)

---

# Grading rubric

| **Component** | **Points** | **Criteria** |
|---|---|---|
| `Q1Mapper` | 20 | Predicate filter correct; emits proper key; correct partial-aggregate math; reuses output objects |
| `Q1Combiner` | 10 | Uses `Q1Value.add`; emits one record per key |
| `Q1Reducer` | 15 | Sums correctly; averages computed (not just summed); output format matches spec |
| 1 GB output correctness | 15 | Exactly 4 rows; keys match `A\|F, N\|F, N\|O, R\|F`; counts match the canonical SF=1 reference values exactly (small rounding differences in averages are fine) |
| 1 TB measurement completed | 30 | Successful step with captured counters (this is the time-intensive part — sticking with it is rewarded heavily) |
| Reflection Q4.1 (compression) | 6 | Demonstrates understanding of cardinality and encoding |
| Reflection Q4.2 (pushdown) | 6 | Distinguishes projection vs predicate pushdown; cites Parquet footer |
| Reflection Q4.3 (predicate selectivity, what-if) | 7 | Correct selectivity computation from counters; predictions distinguish substantially / slightly / about the same with reasoning; correctly identifies the layer that would need to change |
| Reflection Q4.4 (combiner) | 6 | Cites specific numbers; identifies non-combinable query example |
| Reflection Q4.5 (Spark & Iceberg prediction) | 5 | Both predictions written down with a specific direction (faster/slower/same) and reasoned justification; willingness to commit to a number for part (b) |
| **Total** | **120** | |

> **Note:** The due date for this lab is posted on Canvas. Submissions received after that date will receive reduced points in accordance with the course’s late submission policy.

---

# Appendix A: Useful commands

## Watching your spend

```bash
# Current month's cost broken out by service
aws ce get-cost-and-usage \
    --time-period Start=$(date -v1d +%Y-%m-%d),End=$(date +%Y-%m-%d) \
    --granularity MONTHLY --metrics UnblendedCost \
    --group-by Type=DIMENSION,Key=SERVICE \
    --query 'ResultsByTime[*].Groups[*].{Service:Keys[0],Cost:Metrics.UnblendedCost.Amount}' \
    --output table
```

## Listing your active clusters and steps

```bash
aws emr list-clusters --region us-east-1 --active --output table
aws emr list-steps --region us-east-1 --cluster-id <id> --output table
```

## Re-fetching step logs after the step finishes

The EMR console links to logs in S3:

```
s3://<your-log-bucket>/emr-logs/<cluster-id>/steps/<step-id>/
```

The four files you'll consult most:

| **File** | **Contents** |
|---|---|
| `controller.gz` | What EMR did to set up and run the step |
| `stderr.gz` | Your code's stderr — exceptions land here |
| `stdout.gz` | Your code's stdout (usually empty for MR jobs) |
| `syslog.gz` | Hadoop framework log, including counter summaries |

```bash
aws s3 cp s3://<bucket>/emr-logs/<cluster>/steps/<step>/stderr.gz - | gunzip
```

## Resetting an output prefix if you need to re-run

Hadoop's output committer refuses to overwrite. If you need to re-run with the same output path:

```bash
aws s3 rm --recursive s3://$MY_BUCKET/output/q1-1gb/
```

# Appendix B: Common troubleshooting

| **Symptom** | **Likely cause** | **Fix** |
|---|---|---|
| `Access Denied` reading the dataset | Missing `--request-payer requester` (CLI) or `fs.s3a.requester.pays.enabled` (EMR) | Add the flag/config; for EMR, recreate the cluster with the `--configurations` block |
| Step fails immediately with `JAR does not exist` | The `Jar=` field references an S3 URL but EMR can't fetch it | Verify the jar is in your `$MY_BUCKET/jars/` with `aws s3 ls`; check IAM permissions if it lives in a different bucket |
| `Usage: ...` from your driver | Your driver received the wrong number of args | If you passed `MainClass=` to EMR, remove it — the jar's manifest provides it |
| Step succeeds but output rows don't match the expected counts | Most likely an off-by-one in the shipdate filter, wrong key construction, or wrong aggregate math in the mapper | Re-read Exercise 3.1; print one mapper record to stderr and check the values are coming out as you expect |
| Cluster stuck in `Starting` for >15 min | Subnet may not have public IP / IGW route; or quota issue | Check subnet route table; `aws ec2 describe-account-attributes --attribute-names supported-platforms` |
| Cost going up unexpectedly | Cluster still running | `aws emr list-clusters --active` and terminate any stragglers |

---

*End of lab handout.*
