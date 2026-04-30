# Lab 3 — Hadoop MapReduce starter files

This folder contains everything `Lab3_Hadoop_MapReduce.pdf` instructs you to start with:

| File | Role |
|---|---|
| `hadoop.env` | Hadoop config env vars — replaces the Lab 2 version, adds the MapReduce-related fixes for `apache/hadoop:3.4.3`. |
| `docker-compose.yml` | Same cluster as Lab 2 plus a `./workspace:/workspace` bind mount on the namenode for editing Java files from your IDE. |
| `WordCount.java` | Part 2 / Part 4 — custom WordCount over the Project Gutenberg corpus. Two TODO blocks. |
| `StatusCodeCount.java` | Part 3 — count HTTP status codes in the NASA-HTTP 1995 access log. Two TODO blocks. |

## Putting the templates inside the container

The handout sets up a `./workspace` bind mount on the namenode service. Copy the Java starters into that folder so they appear at `/workspace/` inside the container:

```bash
mkdir -p workspace
cp WordCount.java       workspace/
cp StatusCodeCount.java workspace/
```

From there, edit them in your IDE on the host — saves are visible instantly inside the container.

## Compile / package / run cheat sheet

Inside the namenode container (`docker exec -it hadoop-lab-namenode-1 bash`), from `/workspace`:

```bash
cd /workspace
mkdir -p build
javac -cp "$(hadoop classpath)" -d build WordCount.java
jar cf wordcount.jar -C build .
hadoop jar wordcount.jar WordCount \
  /user/student/input/gutenberg \
  /user/student/output/wordcount-custom
```

Substitute `StatusCodeCount` and the NASA paths for Part 3.
