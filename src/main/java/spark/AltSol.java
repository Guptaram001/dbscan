package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class AltSol {

    // Algorithm Parameters [cite: 446]
    static double eps = 0.03;  // Example value (Tune this for your dataset)
    static int minPts = 50;    // Example value

    // ==========================================
    // DATA STRUCTURES
    // ==========================================

    static class Point implements Serializable {
        long id; // Unique ID to track point across partitions
        double x;
        double y;
        int cellId;
        int localClusterId;
        int globalClusterId;
        boolean isLocalRegion; // True if inside core slice, False if ghost
        boolean isCorePoint;
        boolean visited; // Transient for local DBSCAN
        boolean noise;   // Transient for local DBSCAN

        Point(long id, double x, double y) {
            this.id = id;
            this.x = x;
            this.y = y;
            this.localClusterId = 0;
            this.globalClusterId = 0;
            this.isLocalRegion = true;
            this.isCorePoint = false;
            this.visited = false;
            this.noise = false;
        }

        @Override
        public String toString() {
            return x + "," + y + "," + globalClusterId;
        }
    }

    static class PartitionConfig implements Serializable {
        final double minX, maxX, minY, maxY;
        final double cellSize; // 3 * Eps [cite: 240]
        final double buffer;   // 0.1 * Eps [cite: 256]
        final int numCellsX, numCellsY;

        PartitionConfig(double minX, double maxX, double minY, double maxY, double eps) {
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.cellSize = 3 * eps;
            this.buffer = 0.1 * eps;
            this.numCellsX = (int) Math.ceil((maxX - minX) / cellSize) + 1;
            this.numCellsY = (int) Math.ceil((maxY - minY) / cellSize) + 1;
        }
    }

    static class UnionFind {
        Map<String, String> parent = new HashMap<>();

        String find(String x) {
            parent.putIfAbsent(x, x);
            if (!parent.get(x).equals(x)) {
                parent.put(x, find(parent.get(x)));
            }
            return parent.get(x);
        }

        void union(String a, String b) {
            String rootA = find(a);
            String rootB = find(b);
            if (!rootA.equals(rootB)) {
                parent.put(rootA, rootB);
            }
        }
    }

    // ==========================================
    // MAIN ALGORITHM
    // ==========================================

    public static void main(String[] args) {
        // Optimization: Kryo Serialization [cite: 380, 385]
        // Replace Kryo with JavaSerializer
        SparkConf conf = new SparkConf()
                .setAppName("ParallelDBSCAN_PaperCorrected")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // 1. LOAD DATA & ASSIGN UNIQUE IDs
            // We use zipWithIndex to ensure strict identity for merging later.
            String inputPath = "src/main/resources/densired_2.csv"; // Adjust path
            JavaRDD<String> rawLines = sc.textFile(inputPath);

            JavaRDD<Point> points = rawLines.zipWithIndex()     //<String, Long> second is ID
                    .filter(t -> !t._1.trim().isEmpty()).map(t -> {
                String[] parts = t._1.trim().split(",");
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                return new Point(t._2, x, y); // t._2 is the unique Long index
            }).cache();

            if (points.isEmpty()) throw new RuntimeException("No data found.");

            // 2. CALCULATE GLOBAL BOUNDS (Driver side for simplicity, or Reduce)
            // "The data are divided... according to the data range" [cite: 239]
            double minX = points.map(p -> p.x).min(Comparator.naturalOrder());
            double maxX = points.map(p -> p.x).max(Comparator.naturalOrder());
            double minY = points.map(p -> p.y).min(Comparator.naturalOrder());
            double maxY = points.map(p -> p.y).max(Comparator.naturalOrder());

            PartitionConfig config = new PartitionConfig(minX, maxX, minY, maxY, eps);
            Broadcast<PartitionConfig> bcConfig = sc.broadcast(config);

            System.out.println("Grid: " + config.numCellsX + "x" + config.numCellsY +
                    ", CellSize: " + config.cellSize + ", Buffer: " + config.buffer);

            // 3. PARTITIONING WITH SECONDARY EXTENSION [cite: 253-257]
            // "The border of each of the new slices is extended outwards by 0.1 Eps"
            JavaPairRDD<Integer, Point> partitioned = points.flatMapToPair(p -> {
                PartitionConfig cfg = bcConfig.value();
                List<Tuple2<Integer, Point>> assignments = new ArrayList<>();

                // A. Determine Home Cell (Geometric Location)
                int homeX = (int) Math.floor((p.x - cfg.minX) / cfg.cellSize);
                int homeY = (int) Math.floor((p.y - cfg.minY) / cfg.cellSize);
                // Clamp to safe bounds
                homeX = Math.max(0, Math.min(homeX, cfg.numCellsX - 1));
                homeY = Math.max(0, Math.min(homeY, cfg.numCellsY - 1));

                int homeCellId = homeY * cfg.numCellsX + homeX;

                // B. Add to Home Cell (Always Local)
                Point localCopy = new Point(p.id, p.x, p.y);
                localCopy.cellId = homeCellId;
                localCopy.isLocalRegion = true; // [cite: 316]
                assignments.add(new Tuple2<>(homeCellId, localCopy));

                // C. Check Boundaries for Neighbor Replication (Ghost Points)
                // Paper Logic: Replicate if within 0.1 Eps of boundary
                double cellMinX = cfg.minX + homeX * cfg.cellSize;
                double cellMinY = cfg.minY + homeY * cfg.cellSize;

                // Left Neighbor
                if (homeX > 0 && (p.x - cellMinX) <= cfg.buffer) {
                    addGhost(assignments, p, homeY * cfg.numCellsX + (homeX - 1));
                }
                // Right Neighbor
                if (homeX < cfg.numCellsX - 1 && (cellMinX + cfg.cellSize - p.x) <= cfg.buffer) {
                    addGhost(assignments, p, homeY * cfg.numCellsX + (homeX + 1));
                }
                // Bottom Neighbor
                if (homeY > 0 && (p.y - cellMinY) <= cfg.buffer) {
                    addGhost(assignments, p, (homeY - 1) * cfg.numCellsX + homeX);
                }
                // Top Neighbor
                if (homeY < cfg.numCellsY - 1 && (cellMinY + cfg.cellSize - p.y) <= cfg.buffer) {
                    addGhost(assignments, p, (homeY + 1) * cfg.numCellsX + homeX);
                }

                // Note: Corner neighbors (diagonals) can be added similarly if strict Euclidean accuracy
                // is needed at corners, but standard strip implementation usually suffices for 3*Eps grids.

                return assignments.iterator();
            });

            // 4. LOCAL CLUSTERING [cite: 298-320]
            // "Perform local DBSCAN clustering calculations" on each partition
            JavaRDD<Point> clustered = partitioned.groupByKey().flatMap(cell -> {
                List<Point> cellPoints = new ArrayList<>();
                cell._2.forEach(cellPoints::add);

                // Run standard DBSCAN on this slice
                runSerialDBSCAN(cellPoints, eps, minPts);

                return cellPoints.iterator();
            }).cache();

            // 5. MERGING RESULTS [cite: 321-330]
            // "If P is a core point... belonging to both clusters... merge into single cluster"

            // Collect boundary core points to Driver
            // We need any point that is a Core Point and has a Cluster ID
            List<Point> corePoints = clustered
                    .filter(p -> p.isCorePoint && p.localClusterId > 0)
                    .collect();

            // Group by the Unique Point ID to find collisions across partitions
            Map<Long, Set<String>> pointToClusterKeys = new HashMap<>();
            for (Point p : corePoints) {
                // Key format: "CellID_LocalClusterID" (e.g., "5_1")
                String clusterKey = p.cellId + "_" + p.localClusterId;
                pointToClusterKeys.computeIfAbsent(p.id, k -> new HashSet<>()).add(clusterKey);
            }

            UnionFind uf = new UnionFind();

            // Perform Union on sets of size > 1 (Collision detected)
            for (Set<String> keys : pointToClusterKeys.values()) {
                if (keys.size() > 1) {
                    Iterator<String> it = keys.iterator();
                    String root = it.next();
                    while(it.hasNext()) {
                        uf.union(root, it.next());
                    }
                }
            }

            // Generate Global IDs
            Map<String, Integer> clusterKeyToGlobalId = new HashMap<>();
            int nextGlobalId = 1;

            // Resolve all known cluster keys
            // If a key was never merged, it stays as is (find returns itself)
            Set<String> allKeys = new HashSet<>();
            // We must consider ALL local clusters, even those that didn't merge
            List<Point> allClustered = clustered.filter(p -> p.localClusterId > 0).collect();
            for(Point p : allClustered) {
                allKeys.add(p.cellId + "_" + p.localClusterId);
            }

            // Assign IDs to roots
            Map<String, Integer> rootToId = new HashMap<>();
            for (String key : allKeys) {
                String root = uf.find(key);
                if (!rootToId.containsKey(root)) {
                    rootToId.put(root, nextGlobalId++);
                }
                clusterKeyToGlobalId.put(key, rootToId.get(root));
            }

            Broadcast<Map<String, Integer>> bcGlobalMap = sc.broadcast(clusterKeyToGlobalId);

            // 6. GLOBAL CLUSTER GENERATION [cite: 332-335]
            // "Each cluster will have only one global cluster number"
            JavaRDD<Point> finalResult = clustered.map(p -> {
                if (p.localClusterId > 0) {
                    String key = p.cellId + "_" + p.localClusterId;
                    // If map contains key, assign global. Else (rare edge case), keep 0 or handle.
                    p.globalClusterId = bcGlobalMap.value().getOrDefault(key, -1);
                } else if (p.noise) {
                    p.globalClusterId = -1; // Standard noise notation
                } else {
                    p.globalClusterId = 0; // Unassigned
                }
                return p;
            });

            // 7. OUTPUT
            // IMPORTANT: Filter "isLocalRegion" is True [cite: 323]
            // We discard the ghost points now that merging is done.
            finalResult.filter(p -> p.isLocalRegion)
                    .map(Point::toString)
                    .coalesce(1)
                    .saveAsTextFile("output/final_dbscan_result");

            System.out.println("Done. Found " + (nextGlobalId - 1) + " clusters.");

        } finally {
            sc.close();
        }
    }

    // Helper to create ghost points
    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId) {
        Point ghost = new Point(original.id, original.x, original.y);
        ghost.cellId = targetCellId;
        ghost.isLocalRegion = false; // [cite: 316] "False (boundary area)"
        list.add(new Tuple2<>(targetCellId, ghost));
    }

    // ==========================================
    // LOCAL DBSCAN LOGIC (Algorithm 1) [cite: 188]
    // ==========================================

    private static void runSerialDBSCAN(List<Point> points, double eps, int minPts) {
        int clusterIdCounter = 1;

        for (Point p : points) {
            if (p.visited) continue;
            p.visited = true;

            List<Point> neighbors = getNeighbors(points, p, eps);

            if (neighbors.size() < minPts) {
                p.noise = true; // Mark as noise (temporarily) [cite: 168]
            } else {
                p.isCorePoint = true; // [cite: 143]
                expandCluster(points, p, neighbors, clusterIdCounter, eps, minPts);
                clusterIdCounter++;
            }
        }
    }

    private static void expandCluster(List<Point> points, Point p, List<Point> neighbors,
                                      int currentClusterId, double eps, int minPts) {
        p.localClusterId = currentClusterId;

        // Using a queue for efficient expansion [cite: 179-183]
        Queue<Point> seeds = new LinkedList<>(neighbors);

        // To avoid infinite loops in queue processing, we check membership logic
        // In simple Java list DBSCAN, careful management of 'visited' is key.

        while (!seeds.isEmpty()) {
            Point q = seeds.poll();

            // Process unvisited
            if (!q.visited) {
                q.visited = true;
                List<Point> qNeighbors = getNeighbors(points, q, eps);

                if (qNeighbors.size() >= minPts) {
                    q.isCorePoint = true; // It's a core point too
                    // Add neighbors to queue
                    for (Point n : qNeighbors) {
                        if (!n.visited) seeds.add(n);
                        // Note: Depending on logic, you might add visited nodes too if they weren't core,
                        // but standard DBSCAN adds strict unvisited.
                        // Simplified here: we add unvisited to queue.
                    }
                    // Add existing neighbors if they aren't already members of a cluster?
                    // Standard DBSCAN adds neighbors to Seeds.
                    // We cheat slightly: adding all neighbors to queue can blow up memory.
                    // We usually only add to queue if they satisfy density.
                    // Let's stick to: if density >= minPts, join seeds.
                    for (Point n : qNeighbors) {
                        if (n.localClusterId == 0) {
                            n.localClusterId = currentClusterId;
                        }
                        // If we strictly follow the paper Algorithm 1 step 2:
                        // "if n >= MinPts, then these objects are added to the object collection"
                        // This implies adding to the queue.
                        if (!n.visited) { // Optimization
                            seeds.add(n);
                        }
                    }
                }
            }

            // Assignment logic
            if (q.localClusterId == 0 || q.noise) { // Noise can be reclaimed [cite: 168]
                q.localClusterId = currentClusterId;
                q.noise = false;
            }
        }
    }

    private static List<Point> getNeighbors(List<Point> points, Point center, double eps) {
        // Naive O(N) search per point within the partition.
        // Indexing (R-Tree) inside partition is possible but Paper uses simple list scan for "Local DBSCAN".
        List<Point> neighbors = new ArrayList<>();
        double epsSq = eps * eps;
        for (Point p : points) {
            double dx = p.x - center.x;
            double dy = p.y - center.y;
            if (dx*dx + dy*dy <= epsSq) {
                neighbors.add(p);
            }
        }
        return neighbors;
    }
}