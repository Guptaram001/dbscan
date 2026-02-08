# Parallel DBSCAN with Apache Spark

A distributed DBSCAN (Density-Based Spatial Clustering of Applications with Noise) implementation built on Apache Spark. The algorithm uses spatial grid partitioning, local DBSCAN per cell, and merge clusters across cell boundaries.

## Dependencies

- **Java 17** – Required for Spark 4.x (or Java 11 if using Spark 3.x)
- **Apache Spark 4.x** – Distributed computation (e.g. installed via Homebrew: `brew install apache-spark`)
- **Maven 3.6+** – Build tool

### Installing Dependencies (macOS)

```bash
brew install openjdk@17
brew install apache-spark
brew install maven
```

## Setup

1. Clone or extract the project.
2. Set `JAVA_HOME` to Java 17 if it is not your default:
   ```bash
   export JAVA_HOME=/opt/homebrew/opt/openjdk@17
   ```
3. Ensure `spark-submit` and `mvn` are on your `PATH`.

## Compilation

Build the project with Maven:

```bash
mvn clean package -DskipTests
```

This produces `target/TemplateSpark-1.0-SNAPSHOT.jar`, which includes your application code. Spark libraries are marked as `provided` and must be available at runtime.

## Input Format

Input files must be CSV with two numeric columns per line (no header):
- Column 1: latitude (x)
- Column 2: longitude (y)

Example (`src/main/resources/test.csv`):
```
0.1,0.1
0.15,0.15
0.2,0.2
```

## Invocation

### Option 1: Local Mode (single machine)

Runs DBSCAN locally without a Spark cluster:

```bash
./run_local.sh src/main/resources/test.csv
```

### Option 2: Spark Cluster Mode

Uses a standalone Spark master and workers.

**1. Start the Spark Master**

```bash
./start_master.sh
```

- Master UI: http://127.0.0.1:8080
- Press Ctrl+C to stop the master

**2. Start Workers** (in separate terminals)

```bash
./start_worker1.sh
./start_worker2.sh
./start_worker3.sh
```

Each worker listens on ports 8081, 8082, 8083 respectively.

**3. Submit the DBSCAN Job**

```bash
./submit_job.sh src/main/resources/test.csv
```

## Changing the Dataset

To use a different dataset, pass the file path as an argument:

```bash
./submit_job.sh /path/to/your/data.csv
```

The default dataset (when no argument is given) is `src/main/resources/densired_2_shrink.csv`.

**Built-in datasets:**
- `src/main/resources/test.csv` – small demo
- `src/main/resources/densired_2_shrink.csv` – default for cluster runs
- `src/main/resources/densired_2.csv` – full dataset

## Simple Demo

End-to-end demo with the small test dataset:

```bash
# Local mode (no cluster needed)
./run_local.sh src/main/resources/test.csv
```

Or with the cluster:

```bash
# Terminal 1: start master
./start_master.sh

# Terminal 2: start worker
./start_worker1.sh

# Terminal 3: submit job
./submit_job.sh src/main/resources/test.csv
```

## Output

- **results/results.csv** – Summary metrics (runtime, point count, cluster count, etc.)
- **results/edgesToGlobal.csv** – Local-to-global cluster ID mapping (when merge strategy is UF)
- **output/finalClusters&lt;runId&gt;/** – Final clustered points (when DEBUG is enabled)

## Configuration

DBSCAN parameters are set in `EntryPoint.java` via `ExecutionConfiguration`:
- `eps` – Maximum distance (epsilon) for neighbors
- `minPts` – Minimum neighbors to form a core point
- `cellFactor`, `bufferFactor` – Grid partitioning settings
- `mergeStrategy` – `"UF"` (Union-Find) or `"GraphX"`

## Project Structure

```
src/main/java/spark/
├── EntryPoint.java         # Main entry, runs DBSCAN with configured experiments
├── ExecuteDBSCAN.java      # Parallel DBSCAN implementation
├── SerialDBSCAN.java       # Single-machine reference implementation
├── UnionFindMerge.java     # Union-Find cluster merge
├── UnionFindString.java    # Union-Find data structure
├── Utils.java              # Local DBSCAN, KD-Tree, distance helpers
├── Point.java              # Point representation
└── ...
```

## Contributors

- **Mohammed Afaan Sajjad Hussain Shaikh** – Shaikhm@students.uni-marburg.de (3844933)
- **Ram Binay Gupta** – Guptar@students.uni-marburg.de (3848283)
