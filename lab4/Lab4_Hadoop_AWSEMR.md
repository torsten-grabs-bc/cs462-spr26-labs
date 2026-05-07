# CS462 – Big Data Systems — Lab 4: Apache Hadoop on AWS EMR

*Bellevue College — Spring 2026*

| **Course**         | CS462 – Big Data Systems                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------- |
| **Lab Number**     | Lab 4                                                                                              |
| **Topics**         | AWS Academy Learner Lab, Amazon S3, Amazon EMR, Hadoop in the cloud, decoupled storage and compute |
| **Estimated Time** | 2–3 hours (plus AWS provisioning time)                                                             |
| **Points**         | 100                                                                                                |
| **Prerequisite**   | Lab 3 – Apache Hadoop & MapReduce (you will reuse the Gutenberg corpus from that lab)              |
| **Submission**     | Canvas — upload a single ZIP file (see Deliverables section)                                       |

# Learning Objectives

By the end of this lab you will be able to:

  - Sign in to AWS Academy Learner Lab and start a lab session

  - Create an S3 bucket with secure default settings and upload input data through the AWS Console

  - Provision an Amazon EMR Hadoop cluster with the right IAM roles and EC2 instance profile for the Learner Lab environment

  - Configure auto-termination on an EMR cluster to control costs against the $50 Learner Lab budget

  - Submit a Hadoop job as an EMR Step using command-runner.jar — without ever opening an SSH session

  - Read MapReduce results back from S3 and confirm them against the local Hadoop run from Lab 3

  - Recognize and avoid the most common cost and configuration pitfalls of running EMR in a Learner Lab account

# Prerequisites

  - An invitation email from AWS Academy and a working AWS Academy Learner Lab account

  - The five Project Gutenberg .txt files you downloaded for Lab 3 (Shakespeare, Moby-Dick, War and Peace, Pride and Prejudice, Sherlock Holmes), still on your laptop

  - A modern web browser (Chrome, Firefox, Safari, or Edge); no SSH client or local Hadoop install is required for this lab

> ***Note:** If you cannot find your Gutenberg files from Lab 3, the URLs are stable. From a terminal on your laptop you can re-download them with: curl -O https://www.gutenberg.org/cache/epub/${ID}/pg${ID}.txt for IDs 100, 2701, 2600, 1342, and 1661. (On macOS use curl; wget is not installed by default.)*

# Environment Setup

Lab 4 has no local environment to set up — everything runs in your browser against AWS. The only “setup” is signing in to AWS Academy and starting your lab session, which is the first task in Part 1.

Two things worth knowing before you start:

The AWS Academy Learner Lab is region-locked to us-east-1 (N. Virginia). The S3 bucket you create in Part 1 and the EMR cluster you launch in Part 2 must both be in this region, and switching regions in the console will take you to an empty view where nothing you create will be visible.

Each lab session has a four-hour wall-clock budget per session and a $50 lifetime credit budget for the entire term. Most of that credit goes into EMR EC2 instances when a cluster is running. The single most important habit in this lab is making sure you actually shut down resources you create. Every Part below ends with at least one cleanup step — do not skip them.

# Part 1: Connect to AWS Academy and Stage the Corpus in S3 (15 points)

In this part you will sign in to your AWS Academy account, start the lab environment, create an S3 bucket, and upload the same Gutenberg corpus you used in Lab 3.

## 1.1 Sign In and Start the Lab

Look for an email from AWS Academy with instructions on how to create your account and how to sign in. After completing the account setup, navigate to the Canvas site for our AWS Academy Learner Lab. This is a separate Canvas instance from the BC Canvas — the URL is awsacademy.instructure.com:

> https://awsacademy.instructure.com/courses/164692

In the AWS Academy Canvas course page, click into the Modules section and find the entry titled “Launch AWS Academy Learner Lab” — it is roughly halfway down the page. Click it.

The lab page that opens has a control bar at the top with a Start Lab button and a small traffic-light indicator. Click Start Lab. The indicator will turn yellow (provisioning) and then green (ready) after one or two minutes. Once it is green, click the AWS link next to the indicator to open the AWS Management Console in a new browser tab; you are now signed in to a temporary federated session.

## 1.2 Create the S3 Bucket

In the AWS Console, click the All services tile (the small Rubik’s-cube-shaped icon in the navigation bar), choose Storage, then S3. Confirm at the top right of the console that the region selector shows N. Virginia (us-east-1). If it does not, switch to that region before continuing — any bucket you create in another region will be useless to your EMR cluster later.

Click the orange Create bucket button. Make the following choices:

  - AWS Region: US East (N. Virginia) us-east-1 (should already be selected)

  - Bucket name: cs462spr2026-\<yourname\> — bucket names are globally unique across all AWS customers, so include your name or BC username to avoid conflicts

  - Object Ownership: ACLs disabled (recommended) — leave at default

  - Block Public Access settings: leave all four blocks enabled (the default)

  - Bucket Versioning: leave Disabled

  - Default encryption: leave at SSE-S3 with bucket key enabled (the default)

  - Object Lock: leave Disabled

Click Create bucket at the bottom.

> ***Note:** All of these defaults are the right choice for this lab. The Block Public Access settings in particular should stay fully enabled — your EMR cluster reads the bucket through an IAM role, not over the public internet, so making the bucket public would only widen the attack surface without helping the job in any way.*

## 1.3 Create the Folder Structure and Upload the Corpus

Click the bucket name to open it. You should see an empty file listing. Click Create folder, name it gutenberg, and click Create folder again. Open the gutenberg folder, click Create folder once more, and create a subfolder called input. Open input.

Now drag the five Gutenberg .txt files (pg100.txt, pg2701.txt, pg2600.txt, pg1342.txt, pg1661.txt) from a Finder/Explorer window on your laptop directly into the AWS Console window that should show the empty file listing of the Gutenberg/input/ folder in your bucket. Click Upload at the bottom of the dialog and wait for all five files to finish uploading. Together they are roughly 12 MB, so this should take only a few seconds.

Take a screenshot of the S3 console showing all five .txt files inside the gutenberg/input/ prefix. Label it screenshot-1a.png.

## Part 1 — Deliverables

  - screenshot-1a.png — the five Gutenberg .txt files visible in s3://cs462spr2026-\<yourname\>/gutenberg/input/

  - In your write-up: the full S3 URI of your input prefix (you will paste it as an argument to the WordCount step in Part 2)

# Part 2: Create and Configure the EMR Hadoop Cluster (30 points)

In this part you will create an EMR cluster sized appropriately for the Learner Lab budget, register a WordCount step, and configure auto-termination so the cluster cleans itself up after the job finishes.

## 2.1 Open the EMR Console

In the AWS Console, click All services, choose Analytics, then EMR. (If you prefer a direct link: https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1\#/clusters .) Confirm the region in the navigation bar still shows N. Virginia (us-east-1).

Click the orange Create cluster button on the cluster list page.

## 2.2 Name and Application Bundle

In the Name and applications section:

  - Name: cs462-wordcount-\<yourname\>

  - Amazon EMR release: leave the latest 7.x release selected (e.g., emr-7.x.x)

  - Application bundle: click Core Hadoop. From the pre-selected services, deselect everything except Hadoop 3.4.x. (You don’t need Hive, Pig, Hue, or Tez for this lab; removing them shortens provisioning time and reduces the per-hour cost.)

## 2.3 Cluster Configuration: Instance Types

In the Cluster configuration section:

  - Instance type for primary, core, and task nodes: m5.xlarge for all three

  - Number of core instances: leave at the default (1 — more than enough for a 12 MB corpus)

  - Task instance group: 0 — it’s not needed for this course

> ***Note:** The reason for picking m5.xlarge specifically is that it is in the small set of instance types AWS Academy Learner Lab is permitted to launch. If you pick something more exotic (for example r5.xlarge or m7g.xlarge) the cluster will fail to start with a permissions error.*

## 2.4 Add the WordCount Step

Scroll down to the Steps section and click Add step. Fill in:

  - Type: Custom JAR

  - Name: WordCount

  - JAR location: command-runner.jar

  - Arguments (single line, no line breaks): hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordcount s3://cs462spr2026-\<yourname\>/gutenberg/input/ s3://cs462spr2026-\<yourname\>/gutenberg/output/

  - Action on failure: Terminate cluster

Replace \<yourname\> with whatever you used in your bucket name. The S3 URI must exactly match the bucket and prefix you created in Part 1.

> ***Note:** The output prefix (s3://.../gutenberg/output/) must NOT exist when the job runs. Hadoop refuses to write to an existing output path — this is the same behavior you saw in Lab 3 when you re-ran a job without first deleting its HDFS output directory. EMRFS over S3 enforces the same rule.*

## 2.5 Cluster Termination

Scroll to the Cluster termination and node replacement section. Choose Automatically terminate cluster after idle time and set the timeout to 5 minutes. (You could alternatively choose “Terminate after the last step completes” — either works for this lab. Combining Action on failure: Terminate cluster on the step with a 5-minute idle timeout on the cluster gives you the strongest cost guarantee.)

Make sure Termination protection is Off. Termination protection silently overrides auto-termination, so a cluster with both enabled will sit in Waiting state forever, burning credits.

## 2.6 Security Configuration and EC2 Key Pair

Scroll to the Security configuration and EC2 key pair section. Set:

  - Amazon EC2 key pair for SSH to the cluster: vockey

vockey is a key pair AWS Academy pre-creates for you. You can download the matching private key (labsuser.pem) from the lab page’s AWS Details link if you want to SSH into the primary node, but for this lab you do not need to — the WordCount step runs without any SSH involvement.

> ***Note:** If vockey is not in the dropdown list, you are most likely in the wrong region. Switch the region selector at the top of the console to N. Virginia (us-east-1) and the key pair will appear.*

## 2.7 Identity and Access Management

Scroll to the Identity and Access Management (IAM) roles section. This is the single most error-prone configuration on the entire page; read carefully.

  - Service role for Amazon EMR: select EMR\_DefaultRole from the picker

  - EC2 instance profile for Amazon EMR: select EMR\_EC2\_DefaultRole from the picker

  - Custom automatic scaling role: leave blank

> ***Note:** If either of those two roles is missing from the dropdown, try the following:*
> 
> *Open a terminal on your laptop, paste the AWS Academy CLI credentials from the lab page’s AWS Details panel located on the Launch AWS Academy Learner Lab into \~/.aws/credentials on your machine, and run:*
> 
> aws emr create-default-roles
> 
> *This single command should provision both EMR\_DefaultRole (or EMR\_DefaultRole\_V2 on newer accounts) and EMR\_EC2\_DefaultRole with the correct trust policies. After it finishes, refresh the EMR Create cluster page and the roles should appear in the dropdowns.*

Take a screenshot of the IAM roles section showing EMR\_DefaultRole and EMR\_EC2\_DefaultRole both selected. Label it screenshot-2a.png.

> ***Note:** Do NOT pick LabRole in this step. LabRole is a generic Learner Lab IAM role; it works for many AWS services, but it does not have the EC2 permissions that EMR’s service role specifically requires (ec2:RunInstances, ec2:CreateSecurityGroup, etc.). A cluster created with LabRole as the service role will pass validation, then terminate itself moments later with the error “Service role arn:aws:iam::...:role/LabRole has insufficient EC2 permissions”.*

## 2.8 Launch the Cluster

Click Create cluster at the bottom of the page. The cluster summary page opens immediately with the status Starting. Watch the Status column for the next several minutes — the sequence is Starting → Bootstrapping → Running → (your step runs) → Terminating → Terminated.

Take a screenshot of the cluster summary page once the cluster reaches Running state (you will see a green status indicator and the cluster ID j-XXXXXXXXXXXX). Label it screenshot-2b.png.

## Part 2 — Deliverables

  - screenshot-2a.png — IAM roles section showing the two EMR roles selected

  - screenshot-2b.png — cluster summary page in Running state with the Cluster ID visible

  - In your write-up: paste the Cluster ID (j-XXXXXXXXXXXX) and the launch timestamp from the cluster summary page

# Part 3: Run WordCount and Inspect Results (30 points)

In this part you will confirm the WordCount step succeeded, locate the output in S3, and produce a top-10 word list to compare with your Lab 3 result.

## 3.1 Confirm the Step Completed

On the cluster summary page, click the Steps tab. You should see your WordCount step. Wait until it shows Completed (this typically takes four to seven minutes after the cluster reaches Running). While waiting, peek into the Events tab. It will show you state changes of your cluster which gives you a way to see how things progress.

If WordCount shows Failed, click into the failed step and read the logs — they may take a minute or so to populate. The most common failures are a typo in the S3 URI, the output prefix already existing, or the IAM role lacking S3 permissions. Fix the issue, terminate the failed cluster, and re-create.

Take a screenshot of the Steps tab showing both steps Completed. Label it screenshot-3a.png.

## 3.2 Locate the Output in S3

Open a new browser tab and go back to the S3 console. Navigate into your bucket → gutenberg/ → output/. You should see one or more part-r-XXXXX files (typically just part-r-00000 for an input this small) and an empty \_SUCCESS marker file. The presence of \_SUCCESS is Hadoop’s signal that the job completed successfully.

Take a screenshot of the S3 listing of the output/ prefix. Label it screenshot-3b.png.

## 3.3 Read the Top Words

Download part-r-00000 with the S3 console’s Download button instead, then run sort -k2 -rn part-r-00000 | head -10 against the local copy. Consolidate into a single result file if your job created more than one.

Record the top-10 words and their counts. Take a screenshot of the terminal output and label it screenshot-3c.png.

## Part 3 — Deliverables

  - screenshot-3a.png — Steps tab with WordCount marked Completed

  - screenshot-3b.png — S3 output prefix listing showing part-r-00000 and \_SUCCESS

  - screenshot-3c.png — terminal showing the top-10 word list from the EMR run

  - In your write-up: the top-10 (word, count) pairs from your EMR run

# Part 4: Cleanup, Cost Hygiene, and Comparison with Lab 3 (15 points)

This part is short but it is the difference between a $5 lab and a $50 lab. Do not skip it.

## 4.1 Confirm the Cluster Terminated

Go back to the EMR console → Clusters list. Confirm your cluster is in state Terminated (not Waiting). The auto-termination policy you set in 2.5 should have fired roughly five minutes after the WordCount step completed. If the cluster is still in Waiting:

  - Tick the box next to the cluster name and click Terminate at the top right

  - Confirm by clicking Terminate again in the dialog

Take a screenshot of the cluster summary page showing Terminated state. Label it screenshot-4a.png.

## 4.2 Delete the Output Prefix in S3

Re-running the WordCount step without first deleting the output prefix will fail (Hadoop refuses to overwrite). Even if you have no plan to re-run, deleting the output keeps your bucket tidy.

In the S3 console, navigate to your gutenberg/ prefix, select the output/ folder, and click Delete. Confirm by typing permanently delete in the confirmation field.

## 4.3 End the AWS Academy Lab Session

Return to the AWS Academy lab page (the tab where you clicked Start Lab in 1.1). Click End Lab at the top of the page. The traffic-light indicator turns red, which is Learner Lab’s signal that no further charges accumulate against your $50 budget.

> ***Note:** Resources you created (your S3 bucket and its contents) survive across lab sessions, but the AWS Console session does not — the temporary CLI credentials are revoked the moment the indicator turns red. Next time you start the lab, your bucket and Gutenberg files will still be there.*

## 4.4 Compare with Lab 3

In your write-up, compare the top-10 word list from Lab 3 Part 1 (the built-in WordCount you ran in your local Docker Hadoop cluster against the same Gutenberg corpus) with the top-10 word list from your Lab 4 EMR run. They should be identical: same input data, same MapReduce program, same output — just running on different infrastructure.

If they differ, explain why in your write-up.

## Part 4 — Deliverables

  - screenshot-4a.png — cluster status showing Terminated

  - In your write-up: a side-by-side table of the top-10 words from Lab 3 vs. Lab 4, plus a 1–2 sentence note on whether they match (and an explanation if they do not)

# Submission Instructions

Create a ZIP file named lab4\_\<YourLastName\>\_\<YourFirstName\>.zip containing:

  - A write-up document (PDF or DOCX) that includes your name, student ID, the date; a brief description (3–4 sentences) of what you did; the full S3 URI of your input prefix from Part 1; the Cluster ID and launch timestamp from Part 2; the full top-10 word list from your EMR run in Part 3; the Lab 3 vs. Lab 4 top-10 comparison from Part 4; and answers to the reflection questions below

  - All seven screenshots: screenshot-1a, 2a, 2b, 3a, 3b, 3c, 4a

## Reflection Questions

Answer each question in your write-up (3–5 sentences each):

  - In Lab 3, your input data lived in HDFS on the same cluster that ran the MapReduce job; in Lab 4, the data lived in S3 and the compute was a separate transient EMR cluster. Name two operational advantages of this storage/compute decoupling and one new failure mode it introduces. Where does intermediate map output live in each model?

  - Auto-termination in Part 2 had two options: “after the last step completes” and “after N minutes of idle time.” For a graded lab where students may need to inspect the cluster’s web UIs after a job runs, which option would you choose, and why? Give one scenario where the other option would be a better fit.

  - Both your Lab 3 local Hadoop runs and this Lab 4 EMR job refuse to write to an existing output directory. Explain why this is a deliberate design choice (a feature, not a bug), and connect it to the broader idea that batch jobs should be idempotent. What property of the job would you have to verify before it would be safe to relax that restriction?

# Grading Rubric

| **Task / Deliverable**                                      | **Points** | **Grading Criteria**                                                                                                                                   |
| ----------------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Setup — AWS Academy access and S3 bucket created**        | 5          | Lab session started and reaches green; S3 bucket created in us-east-1 with Block Public Access fully enabled and ACLs disabled                         |
| **Part 1 — Gutenberg files staged in S3**                   | 10         | All five .txt files visible in gutenberg/input/; screenshot-1a included; S3 input URI recorded in write-up                                             |
| **Part 2 — EMR cluster created with correct configuration** | 15         | Cluster name, m5.xlarge instances, Core Hadoop with Hadoop 3.4.x only, vockey, and the two EMR roles all set correctly; screenshots 2a and 2b included |
| **Part 2 — cluster successfully launched without errors**   | 15         | Cluster reaches Running state; Cluster ID and launch timestamp recorded in write-up                                                                    |
| **Part 3 — WordCount step completed and output produced**   | 20         | Step shows Completed in the EMR console; output prefix in S3 contains part-r-\* and \_SUCCESS; screenshots 3a and 3b included                          |
| **Part 3 — top-10 list reported**                           | 10         | Full top-10 (word, count) pairs reported in write-up; screenshot-3c included                                                                           |
| **Part 4 — cleanup completed**                              | 10         | Cluster shows Terminated state; output prefix deleted; AWS Academy lab session ended; screenshot-4a included                                           |
| **Part 4 — comparison with Lab 3**                          | 5          | Side-by-side top-10 table comparing Lab 3 and Lab 4 results; correct observation that the lists match (with explanation if they do not)                |
| **Reflection questions**                                    | 10         | All reflection questions answered with a few sentences each                                                                                            |
| **TOTAL**                                                   | **100**    |                                                                                                                                                        |

> ***Note:** The due date for this lab is posted on Canvas. Submissions received after that date will receive reduced points in accordance with the course’s late submission policy.*
