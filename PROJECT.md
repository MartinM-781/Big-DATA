# ECE Big Data Processing — IMDb project

Batch analytics, enrichment and streaming on the
[IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/)
with Apache Spark 3.5 (PySpark batch and Structured Streaming) and Apache Kafka 3.8.

The results and the methodology write-ups are in [REPORT.md](REPORT.md).

## Contributors

| Name | E-mail | Main areas |
| --- | --- | --- |
| Martin Mialaret | martin.mialaret@gmail.com | Environment, data preparation, exploration, enrichment, streaming, open question, documentation |
| Victor Bougro | victor.bougro@edu.ece.fr | Code review and approval of the pull requests, merges into `main` |

## Repository layout

```text
.
├── PROJECT.md              # this file: set-up and operations guide
├── REPORT.md               # results + methodology
├── docker-compose.yml      # Spark (batch/streaming) + Kafka + producer
├── docker/spark/           # Spark image (Kafka connector baked in)
├── requirements.txt        # Python deps installed in the image
├── src/imdb_project/       # shared code (config, Spark session, helpers)
├── jobs/                   # one script per pipeline step (see below)
├── producer/               # Wikimedia EventStreams -> Kafka producer
└── results/                # small CSV/Markdown outputs used by REPORT.md
```

| Step | Script | Engine | Output |
| --- | --- | --- | --- |
| 0 | `jobs/download_imdb.py` | Python | `/data/raw/*.tsv.gz` |
| 1 | `jobs/prepare.py` | Spark | `/data/clean/*`, `/data/dq/issues`, `results/prep/` |
| 2 | `jobs/explore.py` | Spark | `results/explore/` |
| 3a | `jobs/enrich_select.py` | Spark | `/data/enrich/selection_*`, id lists |
| 3b | `jobs/enrich_fetch.py` | Python (HTTP) | `/data/enrich/raw/*.jsonl` |
| 3c | `jobs/enrich_build.py` | Spark | `/data/enrich/films_enriched`, `results/enrich/` |
| 4a | `jobs/stream_lookup.py` | Spark | `/data/stream/lookup` |
| 4b | `producer/wiki_to_kafka.py` | Python | Kafka topic `wiki.recentchange` |
| 4c | `jobs/stream_process.py` | Spark Structured Streaming | `/data/stream/{matches,windows,volume}` |
| 4d | `jobs/stream_report.py` | Spark | `results/stream/` |
| 5 | `jobs/open_question.py` | Spark | `results/open/` |

`/data` is the Docker named volume `imdb-data` (or a host folder, see
`.env.example`). Only the small result tables under `results/` are committed.

## Requirements

* Docker Desktop (or Docker Engine) with Docker Compose v2.
* At least **8 GB of RAM allocated to Docker** (the Spark driver uses 5 GB,
  Kafka 512 MB; change `spark.driver.memory` in
  `docker/spark/spark-defaults.conf` to match your machine; with less memory
  Spark spills to disk and runs slower) and **~25 GB of free disk** for the
  volume. On Windows, the Docker VM is capped in `%USERPROFILE%\.wslconfig`
  (we used `memory=8GB`, `processors=8`).
* Internet access (IMDb download, Wikidata/Wikipedia APIs, Wikimedia
  EventStreams).
* Tested on Windows 11 + Docker Desktop 29 (WSL 2 backend, VM limited to
  8 CPU threads and 8 GB). Linux/macOS work identically.

No local Python, Java or Spark installation is required: everything runs in
containers.

## Step-by-step

All commands are run from the repository root.

### 1. Environment set-up

```bash
cp .env.example .env
```

Edit `.env` and set `WIKI_CONTACT` to a URL or e-mail where you can be
reached: it is sent in the `User-Agent` of every Wikimedia request, as their
policy requires.

```bash
docker compose --profile batch build
```

This builds `ece-imdb-spark:latest` (Spark 3.5.5, Java 17, Python 3 with
`requests`, `pandas`, `kafka-python`, and the Spark-Kafka connector jars).

### 2. Download the IMDb data (~2 GB compressed)

```bash
docker compose run --rm spark python3 jobs/download_imdb.py
```

### 3. Data preparation (about 25–35 min)

```bash
docker compose run --rm spark spark-submit jobs/prepare.py
```

Use `--reuse-staging` to skip TSV decoding on re-runs and `--skip-profile` to
skip the null profiling.

### 4. Data exploration (about 10–15 min)

```bash
docker compose run --rm spark spark-submit jobs/explore.py
```

Run a subset with `--only q1 q5` (`q2` also produces the `q3` tables).

### 5. Data enrichment

```bash
docker compose run --rm spark spark-submit jobs/enrich_select.py
```

```bash
docker compose run --rm spark python3 jobs/enrich_fetch.py
```

```bash
docker compose run --rm spark spark-submit jobs/enrich_build.py
```

The fetcher is rate-limited (default 1 request/s, sequential) and takes about
30–60 minutes for the full scope. It is **resumable**: interrupt it at any time
and re-run the same command. `--limit 50` fetches only a sample for testing.
The scope is parameterised (`enrich_select.py --top-n 0 --principals 0`
selects the full dataset with the same code).

### 6. Streaming (about 20 min)

Build the lookup table (requires steps 3 and 5):

```bash
docker compose run --rm spark spark-submit jobs/stream_lookup.py
```

Start Kafka and the Wikimedia producer in the background. `LATE_FRACTION`
delays a share of the events by `LATE_DELAY` seconds to demonstrate the
watermark:

```bash
LATE_FRACTION=0.02 LATE_DELAY=900 docker compose --profile streaming up -d kafka producer
```

On Windows PowerShell set the variables first:
`$env:LATE_FRACTION="0.02"; $env:LATE_DELAY="900"` then run the command
without the prefix.

Run the streaming job for 15 minutes, then summarise the run:

```bash
docker compose run --rm spark spark-submit jobs/stream_process.py --minutes 15 --reset
```

```bash
docker compose run --rm spark spark-submit jobs/stream_report.py
```

Stop Kafka and the producer:

```bash
docker compose --profile streaming down
```

Useful while it runs: `docker compose logs -f producer`, and the Spark UI at
<http://localhost:4040> if you add `-p 4040:4040` to the `run` command.

### 7. Open question

```bash
docker compose run --rm spark spark-submit jobs/open_question.py
```

### Clean-up

```bash
docker compose --profile streaming --profile batch down -v
```

`-v` deletes the `imdb-data` volume (raw and processed data).

## Code style

Python follows PEP 8 (checked with `flake8`, 88-character lines, see
`setup.cfg`). Markdown follows the Markdown Guide conventions.

## Git workflow

Each task lives on its own feature branch (`feature/<task>`) and is merged into
`main` through a pull request reviewed and approved by another member of the
group. Nobody commits directly to `main`.

## Declaration of AI / LLM usage

| Tool | Used for |
| --- | --- |
| Claude Code (Anthropic, model Claude Opus 5.5) | Scaffolding the Docker/Spark/Kafka environment; drafting the PySpark jobs (preparation, exploration, enrichment, streaming, open question) and the Wikimedia producer/fetcher; drafting this guide and the methodology text in `REPORT.md`. |

All AI-generated code was run end-to-end on the real data, and its outputs
were reviewed by the group. Every number in `REPORT.md` comes from the
`results/` files produced by the jobs, not from the AI. **No machine-generated
text is part of the enriched dataset**: synopses and biographies are copied
from human-authored Wikipedia articles, with their source URL and revision id.
