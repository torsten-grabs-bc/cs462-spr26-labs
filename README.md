# CS 462 — Big Data Systems (Spring 2026)

Lab handouts, starter code, and scripts for **CS 462 Big Data Systems** at Bellevue College, Spring 2026.

## Layout

```
.
├── lab0/    Docker setup — Dockerfile, docker-compose.yml, validate.py
├── lab1/    Big data file formats — per-section script snapshots (see lab1/README.md)
├── lab2/    HDFS fault tolerance — hadoop.env, docker-compose.yml
└── lab3/    Hadoop MapReduce — hadoop.env, docker-compose.yml, WordCount.java, StatusCodeCount.java
```

Each lab folder contains the lab handout (`.pdf`) and the configuration / code files the handout instructs students to create. The filenames match the names used in the handout — students can copy them straight into their working folder rather than re-typing from the PDF.

## How students use this repo

The expected flow is to copy each lab's scaffold files into the working folder the handout names (`lab0/`, `hadoop-lab/`, etc.) and then follow the instructions from the PDF. The scaffolds are starting points — Java files have TODO blocks to fill in, Python files are runnable as-is.

## Notes

- Lecture decks (`.pptx`) are intentionally **not** tracked here — they live in the course materials folder.
- macOS `.DS_Store` files are gitignored.
- Build artifacts (`build/`, `*.class`, `*.jar`) are gitignored; compile inside the container, don't commit binaries.
- `lab2/hadoop.env` and `lab3/hadoop.env` are intentionally different: Lab 3 adds the MapReduce/YARN classpath fixes needed for the `apache/hadoop:3.4.3` image. The Lab 3 docker-compose.yml also adds a `./workspace` bind mount on the namenode for editing Java files.
