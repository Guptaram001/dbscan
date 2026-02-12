# Group 1 Parallel DBSCAN with Apache Spark

A distributed DBSCAN (Density-Based Spatial Clustering of Applications with Noise) implementation built on Apache Spark. The algorithm uses spatial grid partitioning, local DBSCAN per cell, and merge clusters across cell boundaries.

## Dependencies

- **Java 17** – Required (or Java 11 if using Spark 3.x)
- **Apache Spark 3.5.0** – Distributed computation (e.g. installed via Homebrew: `brew install apache-spark`)
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

Example:
```
0.1,0.1
0.15,0.15
0.2,0.2
```

## Invocation

The scripts accept three arguments:
1. **MODE** (optional, default: `Serial`) – Execution mode: `Serial`, `UF`, or `GraphX`
2. **INPUT_FILE** (optional, default varies by script) – Path to input CSV file
3. **DEBUG** (optional, default: `false`) – Enable debug output

### Option 1: Local Mode (single machine)

Runs DBSCAN locally without a Spark cluster:

```bash
# Serial mode (default)
./run_local.sh Serial src/main/resources/densired_2_shrink.csv false

# Or with UF merge strategy
./run_local.sh UF src/main/resources/densired_2_shrink.csv false

# Or with GraphX merge strategy
./run_local.sh GraphX src/main/resources/densired_2_shrink.csv false
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
# Serial mode
./submit_job.sh Serial src/main/resources/densired_2.csv false

# UF merge strategy
./submit_job.sh UF src/main/resources/densired_2.csv false

# GraphX merge strategy
./submit_job.sh GraphX src/main/resources/densired_2.csv false
```

## Changing the Dataset

To use a different dataset, pass the file path as the second argument:

```bash
./submit_job.sh UF /path/to/your/data.csv false
```

**Default datasets:**
- `run_local.sh` defaults to `src/main/resources/densired_2_shrink.csv`
- `submit_job.sh` defaults to `src/main/resources/densired_2.csv`

**Built-in datasets:**
- `src/main/resources/densired_2_shrink.csv` – smaller dataset (default for local runs)
- `src/main/resources/densired_2.csv` – medium dataset (default for cluster runs)
- `src/main/resources/densired_3.csv` – additional dataset
- `src/main/resources/geolife_*.csv` – various sizes of Geolife trajectory data

## Simple Demo

End-to-end demo with a sample dataset:

```bash
# Local mode (no cluster needed) - Serial execution
./run_local.sh Serial src/main/resources/densired_2_shrink.csv false

# Local mode with UF merge strategy
./run_local.sh UF src/main/resources/densired_2_shrink.csv false
```

Or with the cluster:

```bash
# Terminal 1: start master
./start_master.sh

# Terminal 2: start worker
./start_worker1.sh

# Terminal 3: submit job with UF merge strategy
./submit_job.sh UF src/main/resources/densired_2.csv false
```

## Output

Results are written to timestamped directories under `results/`:

- **results/Exec_&lt;runId&gt;/results.txt** – Summary metrics (runtime, point count, cluster count, etc.)
- **results/Exec_&lt;runId&gt;/edgesToGlobal.csv** – Local-to-global cluster ID mapping (when merge strategy is UF)
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
├── EntryPoint.java              # Main entry point for Spark execution
├── SerialEntryPoint.java        # Entry point for serial (non-Spark) execution
├── ExecuteDBSCAN.java           # Parallel DBSCAN implementation
├── SerialDBSCAN.java            # Single-machine reference implementation
├── ExecutionConfiguration.java  # Configuration class for DBSCAN parameters
├── PartitionConfiguration.java   # Grid partitioning configuration
├── UnionFindMerge.java          # Union-Find cluster merge
├── UnionFindString.java         # Union-Find data structure
├── GraphxMerge.java             # GraphX-based cluster merge
├── Utils.java                   # Local DBSCAN, KD-Tree, distance helpers
├── Point.java                   # Point representation
├── Result.java                  # Result metrics container
├── QueryMetrics.java            # Query performance metrics
└── ...
```

## Contributors

- **Mohammed Afaan Sajjad Hussain Shaikh** – Shaikhm@students.uni-marburg.de (3844933)
- **Ram Binay Gupta** – Guptar@students.uni-marburg.de (3848283)
