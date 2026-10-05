# Bronze Data Pipeline

A Python/PySpark project for ingesting batch CSV data into a bronze layer using
Delta Lake. This GitHub-ready code export includes ingestion code in `src/`,
tests in `testing/`, and design notes in `docs/`. Input datasets and the
exploratory notebook are omitted; place local batch CSVs under `data/` when
needed. That directory is ignored by Git.

> **Project status:** This is a work in progress. Several validation and Delta
> writer functions still raise `NotImplementedError`, and some tests are
> unfinished. The repository is not yet a complete end-to-end runnable
> ingestion application.

## Ubuntu setup

The steps below target Ubuntu 24.04 (Python 3.12) and OpenJDK 17. Spark needs a
Java runtime; `requirements.txt` pins the Python package versions used by the
project.

### 1. Install system prerequisites

```bash
sudo apt update
sudo apt install -y git python3 python3-venv openjdk-17-jdk
```

Check the installed versions:

```bash
python3 --version
java -version
```

### 2. Clone the repository

Clone it into a directory named `data_pipeline`. Run pytest from the project
root so the `src` package and other project-level modules are importable.

```bash
git clone https://github.com/<your-username>/<repository-name>.git data_pipeline
cd data_pipeline
```

Replace the URL with the GitHub repository URL after the repository has been
created.

### 3. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

The `.venv/` directory is ignored by Git. If you already keep your virtual
environment outside the repository, activate that environment instead.

### 4. Install project dependencies

```bash
python -m pip install -r requirements.txt
```

If Java is installed but Spark cannot find it, set `JAVA_HOME` for the current
shell:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

On non-amd64 Ubuntu, the Java installation directory may have a different
architecture suffix.

### 5. Run the tests

From the repository root, run:

```bash
python -m pytest testing
```

The tests use a local Spark session. Delta Lake may need to download its JVM
dependencies the first time the Spark session starts, so the initial run needs
network access. Since the project is still under development, expect unfinished
tests or `NotImplementedError` failures until the remaining functions are
implemented.

## Repository layout

```text
Configs/   Entity schemas
data/      Local CSV input batches (not included; ignored by Git)
docs/      Design documentation
src/       Ingestion, metadata, validation, and writer modules
testing/   Pytest tests and shared fixtures
utils/     Shared utilities
```

Review any local CSV inputs for sensitive information before deciding to
version them; this export intentionally excludes the existing datasets.
