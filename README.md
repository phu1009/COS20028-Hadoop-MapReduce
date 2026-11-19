# COS20028 – Big Data Architecture: Hadoop MapReduce, Pig & Hive

Assignments for **COS20028 Big Data Architecture and Application** at Swinburne University of Technology (Hanoi), June – August 2025.

## Contents
| Path | Description |
| --- | --- |
| [`docs/DinhPhuNguyen_COS20028_Assignment1_2025.pdf`](docs) | Assignment 1 – MapReduce application: design flowchart, Mapper/Reducer implementation (word count) and answers, run on the Cloudera Hadoop VM |
| [`src/`](src) | Assignment 2 – Java MapReduce jobs over the AUSTLANG (Australian Indigenous languages) dataset |
| [`docs/COS20028_Assignment2_105027282.docx`](docs) | Assignment 2 report: Pig Latin multi-file analysis (`LOAD` with wildcards, Piggybank), HiveQL external tables and duplicate checks, MapReduce data-cleaning jobs |
| [`docs/COS20028_Assignment2_Code_105027282.docx`](docs) | Full source listing submitted with the report |

### MapReduce jobs (`src/`)
| Task | Job | Purpose |
| --- | --- | --- |
| 2.4 | `LngStJob` | Map-only job extracting language ↔ state pairs |
| 2.5 | `WordDriver` / `WordMapper` / `WordReducer` | Classic word-count MapReduce |
| 3.2 | `LngIdJob` | Map-only job extracting language codes / IDs (uniqueness check) |
| 4.2 | `LngNameJob`, `LngSynonymJob`, `LngThlJob` | Split multi-valued name, synonym and thesaurus fields into normalised records |
| 4.2 | `RelCodeNameJob`, `RelCodeStJob`, `RelCodeSynonymJob`, `RelCodeThpJob` | Build relationship tables keyed by language code |

## Tech stack
Hadoop (HDFS, MapReduce, Java API) · Apache Pig (Pig Latin) · Apache Hive (HiveQL) · Cloudera VM

## Build & run (example)
```bash
javac -classpath `hadoop classpath` -d classes src/task2.5/*.java
jar cvf wordcount.jar -C classes .
hadoop jar wordcount.jar stubs.WordDriver <input> <output>
```

## Notes
- The AUSTLANG dataset (AIATSIS) is not redistributed here.
- Commit dates reflect the original file modification dates of the coursework; the repository itself was published on GitHub in November 2026.
