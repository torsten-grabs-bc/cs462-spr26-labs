# CS462 – Big Data Systems

Lab 8: Spark at Scale — TPC-H Q1 on EMR with Spark Connect

Bellevue College  –  Spring 2026

| **Course** | CS462 – Big Data Systems |
| --- | --- |
| **Lab Number** | Lab 8 |
| **Topics** | Apache Spark on Amazon EMR, Spark Connect, scaling data and compute, Spark vs. MapReduce, AWS Academy Learner Lab |
| **Estimated Time** | ~3 hours, much of it unattended — the 1 TB runs take a while, so start a run and work on your writeup while it finishes |
| **Points** | 100 (+20 bonus available) |
| **Prerequisite** | Lab 5 (EMR MapReduce on TPC-H — for the comparison) and Lab 6 (Spark Q1 in Docker — you reuse that notebook) |
| **Submission** | Canvas — upload a single ZIP file (see Submission section) |

# Learning objectives

By the end of this lab you will be able to:

- Run the **same** Spark code on your laptop and on a remote EMR cluster by changing a single line, using **Spark Connect**.
- Launch and size Amazon EMR clusters from the AWS Academy Learner Lab shell.
- Measure and compare query runtime as you scale **data** (1 GB → 1 TB) and **compute** (one node → a multi-node cluster).
- Explain *why* the numbers move the way they do, and compare Spark against your Lab 5 MapReduce result on identical hardware.

# The big idea: same notebook, remote engine

In Lab 6 you ran TPC-H Q1 in your local Docker Spark against 1 GB of Parquet in S3. In this lab you keep that exact notebook and query — you only change the line that creates the Spark session, pointing it at a **Spark Connect server** running on an EMR cluster:

```python
# Lab 6 (local):
spark = SparkSession.builder.config(...).getOrCreate()

# Lab 8 (remote on EMR):
spark = SparkSession.builder.remote("sc://<PRIMARY_PUBLIC_DNS>:15002").getOrCreate()
```

Your notebook stays on your laptop; all execution and all S3 reads happen on the cluster. Credentials never leave the cluster, so you don't configure any AWS keys in the notebook.

# What you will measure

You will fill in this table (you already have rows A and the Lab 5 row from previous labs):

| Run | Engine & cluster | Executor vCPUs | Data | Time |
|-----|------------------|----------------|------|------|
| A | Spark — Docker on your laptop (Lab 6) | your laptop's cores | 1 GB | _from Lab 6_ |
| B | **EMR Spark** — single `m5.xlarge` | 4 | 1 GB | _____ |
| C | **EMR Spark** — single `m5.xlarge` | 4 | 1 TB | _____ |
| D | **EMR Spark** — 1× `m5.xlarge` + 3× `m5.2xlarge` core | 24 | 1 TB | _____ |
| (L5) | **EMR Hadoop** (Lab 5) — same hardware as D | 24 | 1 TB | _from Lab 5_ |

(Executor vCPUs = the cores actually doing query work. On the EMR clusters the primary node doesn't run executors, so Run D's 24 comes from the three `m5.2xlarge` core nodes — 3 × 8 — not the primary.)

Throughout this lab, your Lab 5 clusters are the **EMR Hadoop** clusters and the clusters you build here are the **EMR Spark** clusters — keep them apart when you read your results and when you take the terminated-clusters screenshot.

Runs B and C share **one** single-node EMR Spark cluster (you just change the data path). Run D uses a second, larger EMR Spark cluster with the **same hardware as your Lab 5 EMR Hadoop cluster** — same instance types and counts, but with the Spark application instead of Hadoop-only. That matched hardware is what makes D vs. L5 a clean EMR-Spark-vs-EMR-Hadoop comparison.

# Prerequisites

- Your Lab 6 Docker JupyterLab environment, working and able to read `s3a://tpch-torstengrabs-parquet/1GB/lineitem/`.
- An AWS Academy Learner Lab session started, with the browser **AWS CLI shell** open.
- Your `labsuser.pem` (from the lab's **AWS Details**) in case you need to inspect a cluster.

> You do **not** need to run any AWS CLI on your laptop. Everything cluster-related happens in the Learner Lab shell; everything query-related happens in your local notebook.

---

# Part 1 — Prepare your Lab 6 notebook for Spark Connect

**1.1** In your Lab 6 Docker JupyterLab, add the Spark Connect client libraries (one time). In a cell:

```python
import sys, subprocess
subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                "grpcio", "grpcio-status", "protobuf>=4.21", "googleapis-common-protos"])
```

Then **restart the kernel**. (Your image already has PySpark 3.5.3; that client talks to the EMR 3.5.6 server fine — no version change needed.)

**1.2** You will create the session with `.remote(...)` once you have a cluster (Part 2). Keep your Q1 from Lab 6 ready. **Your choice:** use Spark SQL *or* the DataFrame API — whichever you wrote in Lab 6. Use the **same** query and the **same** date predicate for every run, or your comparison is meaningless.

**1.3** Standard timing. To get numbers that are comparable across the whole class, time **one cold action** on the full Q1 result, with **no caching**:

```python
import time
# ... define your Q1 result DataFrame as `q1` (SQL or DataFrame API) ...
t0 = time.time()
rows = q1.collect()                 # single action that forces the whole job
elapsed = time.time() - t0
print(f"server={spark.version}  groups={len(rows)}  time={elapsed:.1f}s")
for r in rows:
    print(r)
```

Rules: fresh session (cold), do **not** call `.cache()`, trigger the job with exactly **one** action, and record `elapsed`. Printing `spark.version` documents that it ran on the EMR server (`3.5.6-amzn-1`).

---

# Part 2 — Single-node cluster, 1 GB (Run B) — and your YARN check

## 2.1 Launch the cluster (Learner Lab shell)

```bash
REGION=us-east-1
SUBNET=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true \
  --query 'Subnets[0].SubnetId' --output text --region $REGION)

aws emr create-cluster --region $REGION \
  --name "cs462-lab8-$USER" \
  --release-label emr-7.12.0 \
  --applications Name=Spark \
  --service-role EMR_DefaultRole \
  --ec2-attributes "{\"InstanceProfile\":\"EMR_EC2_DefaultRole\",\"SubnetId\":\"$SUBNET\",\"KeyName\":\"vockey\"}" \
  --instance-groups 'InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1' \
  --configurations '[{"Classification":"core-site","Properties":{"fs.s3.useRequesterPaysHeader":"true","fs.s3a.requester.pays.enabled":"true"}}]' \
  --steps '[{"Type":"CUSTOM_JAR","Name":"StartSparkConnect","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Args":["bash","-c","export SPARK_LOG_DIR=/tmp/spark-connect-logs && mkdir -p /tmp/spark-connect-logs && /usr/lib/spark/sbin/start-connect-server.sh --master yarn --packages org.apache.spark:spark-connect_2.12:3.5.6"]}]'
```

Notes:
- `EMR_DefaultRole` / `EMR_EC2_DefaultRole` are required (as in Lab 4/5). Do **not** use `LabRole` as the service role.
- The `--configurations` block turns on **requester-pays** so the cluster can read the shared bucket.
- The `StartSparkConnect` step starts the Spark Connect server on port 15002. The `SPARK_LOG_DIR` override is required — without it the server can't write its log and silently fails to start.

Record the cluster id it prints as `$CLUSTER`, then wait until it is running:

```bash
CLUSTER=j-XXXXXXXX
aws emr wait cluster-running --cluster-id $CLUSTER --region $REGION
```

## 2.2 Open the Spark Connect port to your machine

```bash
# primary node public DNS -> use as <DNS> in your notebook
aws emr describe-cluster --cluster-id $CLUSTER --region $REGION \
  --query 'Cluster.MasterPublicDnsName' --output text

# the primary's security group
MASTER_SG=$(aws emr describe-cluster --cluster-id $CLUSTER --region $REGION \
  --query 'Cluster.Ec2InstanceAttributes.EmrManagedMasterSecurityGroup' --output text)
```

Find the public IP your machine presents to AWS — in a **notebook cell**:

```python
import urllib.request
print(urllib.request.urlopen("https://checkip.amazonaws.com").read().decode().strip())
```

Authorize that IP on port 15002 (use a `/32`; if you are on a VPN that rotates IPs, use the surrounding `/24`):

```bash
aws ec2 authorize-security-group-ingress --region $REGION \
  --group-id $MASTER_SG --protocol tcp --port 15002 --cidr <YOUR_IP>/32
```

## 2.3 Connect and validate (this is your YARN check)

In your notebook, create the remote session and run a tiny sanity check **before** doing real work:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://<DNS>:15002").getOrCreate()
print("server:", spark.version)        # expect 3.5.6-amzn-1
print(spark.range(5).count())           # expect 5  -> the YARN-backed server is healthy
```

If `spark.range(5).count()` returns `5`, your Spark Connect server and its YARN backend are working — you're cleared to run the timed queries. If it hangs, see **Troubleshooting** at the end.

## 2.4 Run B — Q1 on 1 GB

Read the 1 GB lineitem, define your Q1, and run the timing cell from Part 1:

```python
li = spark.read.parquet("s3://tpch-torstengrabs-parquet/1GB/lineitem/")
# Build your Lab 6 Q1 from here, then run the Part 1 timing cell on the result.
```

Use your Q1 from Lab 6 — your choice of the **SQL** path or the **DataFrame** path (the DataFrame API works off `li` directly).

Record the time as **Run B**.

## 2.5 Save your Run B notebook

Save a copy of the notebook with the executed cells (timing output visible): `lab8_runB_1GB.ipynb`. **Keep this cluster running** — you'll reuse it for Run C.

---

# Part 3 — Same node, 1 TB (Run C)

On the **same** single-node cluster and session, change only the data path to the 1 TB set and re-run. (No `.cache()`, fresh read — the 1 TB data is different data, so nothing is cached from Run B.)

```python
li = spark.read.parquet("s3://tpch-torstengrabs-parquet/1TB/lineitem/")
# Same Q1 and timing cell as Run B.
```

Expect this to take a while (tens of minutes) — that is the point. While it runs, write down a prediction: 1 GB → 1 TB is 1000× the data on the same machine — do you expect 1000× the time? Why or why not?

Record it as **Run C**, save `lab8_runC_1TB.ipynb`, then terminate this cluster from the Learner Lab shell:

```bash
aws emr terminate-clusters --cluster-id $CLUSTER --region $REGION
```

---

# Part 4 — Scale the cluster, 1 TB (Run D = same hardware as your Lab 5 EMR Hadoop cluster)

Launch a second EMR Spark cluster with the **same hardware as your Lab 5 EMR Hadoop cluster** — same instance types and counts (1× `m5.xlarge` primary + 3× `m5.2xlarge` core), but with Spark instead of Lab 5's Hadoop-only bundle. Use the same launch command as 2.1 (which already requests Spark), with one changed line for the instance groups:

```bash
  --instance-groups \
    'InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1' \
    'InstanceGroupType=CORE,InstanceType=m5.2xlarge,InstanceCount=3' \
```

(Everything else — release, applications, roles, `--configurations`, and the `StartSparkConnect` step — is the same.)

Then repeat Part 2.2–2.3 for the new cluster: get the **new DNS**, point your notebook at it, and run the sanity check, then run the **same** Q1 on the **1 TB** data (Part 3's path). Record it as **Run D**, save `lab8_runD_1TB.ipynb`, and **terminate the cluster** when done.

> **You usually don't need to re-open port 15002 for this cluster.** EMR reuses a single managed security group (`ElasticMapReduce-master`) for every cluster in your VPC, so the 15002 rule you added in Part 2.2 is still there and already applies. If you re-run `authorize-security-group-ingress` you'll just get a harmless `InvalidPermission.Duplicate` message — ignore it. (You only need a new rule if your VPN egress IP has moved outside the range you authorized.)

> **Budget/capacity note:** one `m5.xlarge` (single node) plus this 1+3 cluster would together hit the Learner Lab's 32-vCPU ceiling exactly. Simplest is to finish Parts 2–3 and **terminate** the single-node cluster before launching Part 4. Don't leave clusters idle — they spend your credit.

---

# Part 5 — Results and analysis

**5.1** Complete the table (A and L5 come from Labs 6 and 5):

| Run | Engine & cluster | Executor vCPUs | Data | Time |
|-----|------------------|----------------|------|------|
| A | Spark — Docker laptop | your laptop's cores | 1 GB | |
| B | EMR Spark — single `m5.xlarge` | 4 | 1 GB | |
| C | EMR Spark — single `m5.xlarge` | 4 | 1 TB | |
| D | EMR Spark — 1×`m5.xlarge` + 3×`m5.2xlarge` | 24 | 1 TB | |
| L5 | EMR Hadoop (Lab 5) | 24 | 1 TB | |

**5.2** Answer briefly (a few sentences each):

1. **A vs B:** Both run 1 GB, but B is "a real cluster." Is B faster than your laptop? Explain what dominates the time at 1 GB.
2. **B vs C:** You multiplied the data by 1000 on the same machine. Did the time grow 1000×? Where did the time actually go — reading from S3, or computing the aggregate? How can you tell?
3. **C vs D:** Same 1 TB data, but more compute. Reason in **total executor vCPUs** (see the table — 4 vs 24), not node count: the core nodes are larger than the single node, so this is not simply "3× the nodes." What speedup did you get, and how close was it to the increase in executor vCPUs? If it isn't exactly proportional, why?
4. **D vs L5:** Same hardware, same data, same query — your EMR Spark cluster vs. your Lab 5 EMR Hadoop cluster. Which won, and by how much? Give two reasons the two engines differ on this workload.
5. Q1 reads almost all of `lineitem` no matter how selective the date filter is. If you wanted the 1 TB run to finish faster **without** adding nodes, what could you change about how the data is stored — and what would that cost you in terms of comparing against Lab 5?

---

# Deliverables

1. Three executed notebooks with visible timing output: `lab8_runB_1GB.ipynb`, `lab8_runC_1TB.ipynb`, `lab8_runD_1TB.ipynb`. Each must show `spark.version` (proving it ran on EMR) and the timing line.
2. A short writeup (PDF or Markdown) containing the completed results table (including your Lab 5 row) and your answers to the five analysis questions.
3. A **screenshot of the EMR console Clusters page** taken at the end of the lab, showing **all** your clusters in the `Terminated` state (no cluster left `Running` or `Waiting`).

# Submission

Submit a single zip file named `lab8_<yourname>.zip` containing:

```
lab8_<yourname>.zip
├── lab8_runB_1GB.ipynb          (Run B — EMR, 1 GB)
├── lab8_runC_1TB.ipynb          (Run C — EMR single node, 1 TB)
├── lab8_runD_1TB.ipynb          (Run D — Lab 5-hardware cluster, 1 TB)
├── writeup.md                   (or writeup.pdf — results table + analysis answers 1–5)
└── terminated_clusters.png      (EMR console showing all clusters Terminated)
```

Do not include: your Docker image, the `docker-compose.yml`, `.ipynb_checkpoints/`, the Parquet data (it lives in S3, not your zip), or any cached Spark output.

## Submission checklist

- ☐ All three notebooks connect via Spark Connect and show `spark.version` = `3.5.6-amzn-1`
- ☐ Each notebook's Q1 produces the 4 canonical rows — (A,F), (N,F), (N,O), (R,F) — with a recorded cold time
- ☐ Run D used 1× m5.xlarge + 3× m5.2xlarge core (same hardware as Lab 5)
- ☐ Results table has all five rows (A, B, C, D, L5)
- ☐ Analysis answers 1–5 included
- ☐ Screenshot shows every cluster `Terminated` (none `Running`/`Waiting`)
- ☐ Zip is named `lab8_<yourname>.zip`

# Grading rubric

| Component | Points | Criteria |
|---|---|---|
| Run B — Q1 on EMR, 1 GB (notebook) | 20 | Session connects to the EMR Spark Connect server (shows `spark.version` = 3.5.6-amzn-1); Q1 produces the 4 canonical rows; cold time recorded |
| Run C — Q1 on EMR, 1 TB single node (notebook) | 20 | Same query and predicate on the 1 TB data; `spark.version` shown; cold time recorded |
| Run D — Q1 on the Lab 5-hardware cluster, 1 TB (notebook) | 20 | 1× m5.xlarge + 3× m5.2xlarge core; same query; cold time recorded |
| Results table | 15 | All five rows complete (A, B, C, D, and your Lab 5 result) |
| Analysis answers (1–5) | 20 | Each answered correctly and with reasoning |
| Terminated-clusters screenshot | 5 | EMR console Clusters page showing all clusters `Terminated` (none `Running`/`Waiting`) |
| **Total (required)** | **100** | |
| Bonus (one of the optional tasks above) | +20 | Scaling-curve run with plot, or warm-run comparison |
| **Maximum (with bonus)** | **120** | |

Note: The due date for this lab is posted on Canvas. Submissions received after that date will receive reduced points in accordance with the course's late submission policy.

# Optional (bonus — 20 points)

Do **one** of the following for full bonus credit:

- Add a fourth EMR run with **2** core nodes for a three-point scaling curve (single → 2-core → 3-core at 1 TB), plot time vs. core count, and comment on the trend.
- Add a **warm** run (second execution in the same session) and explain the difference from the cold run.

---

# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `.range(5)` / first call hangs | Port 15002 not open to your **current** IP (VPN rotated), or wrong `<DNS>` | re-check your IP (the `checkip` cell), re-run `authorize-security-group-ingress`; consider authorizing the `/24` |
| First call: `Connection refused` | Reached the host but the Connect server isn't listening | the server failed to start — check the step, then re-start it (below) |
| `StartSparkConnect` step shows COMPLETED but nothing listens on 15002 | the launcher returns 0 even when the server dies (usually the `SPARK_LOG_DIR` permission issue) | SSH in (`ssh -i labsuser.pem hadoop@<DNS>`) and run: `export SPARK_LOG_DIR=/tmp/spark-connect-logs && mkdir -p $SPARK_LOG_DIR && /usr/lib/spark/sbin/start-connect-server.sh --master yarn --packages org.apache.spark:spark-connect_2.12:3.5.6` ; confirm with `sudo ss -tlnp \| grep 15002` (expect `*:15002`) |
| Sanity check hangs only on a single-node cluster (YARN has no free containers) | rare single-node YARN capacity issue | restart the server with `--master 'local[*]'` instead of `--master yarn` (single node uses all 4 cores either way) |
| `Access Denied` reading S3 | requester-pays not set | confirm the `--configurations` core-site block was included at launch |
| Version/protocol error on connect | client/server skew | in the container: `pip install "pyspark[connect]==3.5.6"`, restart kernel |

**Always terminate your clusters when finished:** `aws emr terminate-clusters --cluster-id $CLUSTER --region $REGION`.
