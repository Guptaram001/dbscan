package spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;

public class TestUnionFind {
    static double eps = 0.03;
    static int minPts = 50;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("DBSCAN Analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = args.length >=1? args[0]: "src/main/resources/densired_2_shrink.csv";
        //JavaRDD<String> rawLines = sc.textFile(args[0]);

//        SparkConf conf = new SparkConf().setAppName("DBSCAN Analysis");
//        String inputPath = args.length > 0 ? args[0] : "densired_2_shrink.csv";
//        JavaRDD<String> rawLines = sc.textFile(inputPath);
//        SparkConf conf = new SparkConf().setAppName("DBSCAN Analysis").setMaster("local[*]").set("spark.driver.maxResultSize", "4g");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> rawLines = sc.textFile("src/main/resources/densired_2_shrink.csv");
//        JavaRDD<String> rawLines = sc.textFile("src/main/resources/test.txt");

        LongAccumulator neighborQueryCount = sc.sc().longAccumulator("neighborQueryCount");
        LongAccumulator neighborQueryTimeNs = sc.sc().longAccumulator("neighborQueryTimeNs");


        //Reads from the file and associates each with a long index as Point(index, lat, long, clusterid =0)
        JavaRDD<Point> points = sc.textFile(inputPath)
                .zipWithIndex()
                .filter(t -> !t._1.trim().isEmpty())
                .map(t -> {
                    String[] parts = t._1.trim().split(",");
                    double x = Double.parseDouble(parts[0]);
                    double y = Double.parseDouble(parts[1]);
                    return new Point(t._2, x, y, 0);
                })
                .cache();
        points.take(20).forEach(point ->System.out.println(point));


        long totalPoints = points.count();
        System.out.println("Total points: " + totalPoints);

        String runId = String.valueOf(System.currentTimeMillis());

        // Find min/max coordinates of entire dataset
        double minLatitude = points.map(p -> p.latitude).reduce(Double::min);
        double maxLatitude = points.map(p -> p.latitude).reduce(Double::max);
        double minLongitude = points.map(p -> p.longitude).reduce(Double::min);
        double maxLongitude = points.map(p -> p.longitude).reduce(Double::max);

        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(minLatitude, maxLatitude, minLongitude, maxLongitude, eps);
        final Broadcast<PartitionConfiguration> broadcastPartitionConf = sc.broadcast(partitionConfiguration);

        JavaPairRDD<Integer, Point> partitionedToCellsRDD = points.flatMapToPair(p -> {
            PartitionConfiguration cfg = broadcastPartitionConf.value();
            List<Tuple2<Integer, Point>> assignments = new ArrayList<>();
            //Determine Home Cell (Geometric Location)
            int homeX = (int) Math.floor((p.latitude - minLatitude) / cfg.cellSize);
            int homeY = (int) Math.floor((p.longitude - minLongitude) / cfg.cellSize);
            // Fix to safe bounds
            homeX = Math.max(0, Math.min(homeX, cfg.numCellsX - 1));
            homeY = Math.max(0, Math.min(homeY, cfg.numCellsY - 1));
            int homeCellId = homeY * cfg.numCellsX + homeX;
            // Add to Home Cell (Always Local)
            Point local = new Point(p.id, p.latitude, p.longitude, 0);
            local.cellId = homeCellId;
            local.isLocalRegion = true;
            assignments.add(new Tuple2<>(homeCellId, local));
            // Check Boundaries for Neighbor Replication (Ghost Points)
            double cellMinX = minLatitude + homeX * cfg.cellSize;
            double cellMinY = minLongitude + homeY * cfg.cellSize;

            double dxLeft = p.latitude - cellMinX;
            double dxRight = (cellMinX + cfg.cellSize) - p.latitude;
            double dyBottom = p.longitude - cellMinY;
            double dyTop = (cellMinY + cfg.cellSize) - p.longitude;

            // Left Neighbor
            if (homeX > 0 && dxLeft <= cfg.buffer) {
                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX - 1));
            }
            // Right Neighbor
            if (homeX < cfg.numCellsX - 1 && dxRight <= cfg.buffer) {
                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX + 1));
            }
            // Bottom Neighbor
            if (homeY > 0 && dyBottom <= cfg.buffer) {
                addGhost(assignments, p, (homeY - 1) * cfg.numCellsX + homeX);
            }
            // Top Neighbor
            if (homeY < cfg.numCellsY - 1 && dyTop <= cfg.buffer) {
                addGhost(assignments, p, (homeY + 1) * cfg.numCellsX + homeX);
            }

            // Top-Left (homeX-1, homeY+1)
            if (homeX > 0 && homeY < cfg.numCellsY - 1 && dxLeft <= cfg.buffer && dyTop <= cfg.buffer) {
                int cellId = (homeY + 1) * cfg.numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId);
            }

            // Top-Right (homeX+1, homeY+1)
            if (homeX < cfg.numCellsX - 1 && homeY < cfg.numCellsY - 1 && dxRight <= cfg.buffer && dyTop <= cfg.buffer) {
                int cellId = (homeY + 1) * cfg.numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId);
            }

            // Bottom-Left (homeX-1, homeY-1)
            if (homeX > 0 && homeY > 0 && dxLeft <= cfg.buffer && dyBottom <= cfg.buffer) {
                int cellId = (homeY - 1) * cfg.numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId);
            }

            // Bottom-Right (homeX+1, homeY-1)
            if (homeX < cfg.numCellsX - 1 && homeY > 0 && dxRight <= cfg.buffer && dyBottom <= cfg.buffer) {
                int cellId = (homeY - 1) * cfg.numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId);
            }
            return assignments.iterator();
        });

        System.out.println("Points Partitioned based on home cell ");
        partitionedToCellsRDD.take(20).forEach(pair -> System.out.println(pair._1()+" "+pair._2()));

        //Groups each points based on the cells they belong to and execute DBSCAN locally.
        JavaPairRDD<Integer, Point> dbscanClusteredAsPerCellsRDD =
                partitionedToCellsRDD
                        .groupByKey()
                        .flatMapToPair(cell -> {

                            int cellId = cell._1;
                            List<Point> cellPoints = new ArrayList<>();
                            cell._2.forEach(cellPoints::add);

                            localDBSCAN(cellPoints, eps, minPts, neighborQueryCount, neighborQueryTimeNs);

                            List<Tuple2<Integer, Point>> out = new ArrayList<>();
                            for (Point p : cellPoints) {
                                out.add(new Tuple2<>(cellId, p));
                            }
                            return out.iterator();
                        })
                        .cache();
        System.out.println("Local DBSCAN Executed on each cells they belong ");
        dbscanClusteredAsPerCellsRDD.take(20).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));


        JavaPairRDD<Double, Iterable<Point>> groupedByPoint =
                dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2))
                        .groupByKey();
        System.out.println("Grouped by points by after local DBSCAN executed on each cells to initiate merge ie Same point in multiple cells");
        groupedByPoint.take(200).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));


        JavaPairRDD<String, String> samePointMergeRDD =
                groupedByPoint.flatMapToPair(entry -> {

                    List<Point> pts = new ArrayList<>();
                    entry._2.forEach(pts::add);

                    List<Tuple2<String, String>> merges = new ArrayList<>();

                    for (int i = 0; i < pts.size(); i++) {
                        for (int j = i + 1; j < pts.size(); j++) {
                            Point a = pts.get(i);
                            Point b = pts.get(j);

                            if (a.clusterId <= 0 || b.clusterId <= 0)
                                continue;
                            if (!(a.isCorePoint || b.isCorePoint))
                                continue;

                            double dist = distance(a, b);
                            if (dist <= eps) {
                                String keyA = a.cellId + "_" + a.clusterId;
                                String keyB = b.cellId + "_" + b.clusterId;
                                if (!keyA.equals(keyB))
                                    merges.add(new Tuple2<>(keyA, keyB));
                            }
                        }
                    }
                    return merges.iterator();
                });

        System.out.println("Merging points that are boundary and core points in different cells");
        samePointMergeRDD.take(200).forEach(pair -> System.out.println("Edges: "+pair._1() + ": " + pair._2()));




//        // ============================================================
//// DISTRIBUTED UNION-FIND (BEST OF BOTH WORLDS)
//// ============================================================
//
//// Step 1: Collect merge edges (small enough to fit in driver memory)
//        List<Tuple2<String, String>> mergeEdges = samePointMergeRDD.collect();
//
//// Step 2: Run Union-Find on driver (fast for small graphs)
//        UnionFindString uff = new UnionFindString();
//        for (Tuple2<String, String> e : mergeEdges) {
//            uff.union(e._1, e._2);
//        }
//
//// Step 3: Get ALL cluster keys from all points (including isolated)
//        Set<String> allClusterKeys = dbscanClusteredAsPerCellsRDD
//                .values()
//                .filter(p -> p.clusterId > 0)
//                .map(p -> p.cellId + "_" + p.clusterId)
//                .distinct()
//                .collect()
//                .stream()
//                .collect(Collectors.toSet());
//
//// Step 4: Build complete mapping: localKey → representative
//        Map<String, String> keyToRoot = new HashMap<>();
//        for (String k : allClusterKeys) {
//            keyToRoot.put(k, uff.find(k));
//        }
//
//        System.out.println("Total cluster keys: " + keyToRoot.size());
//
//// Step 5: Map representatives to global IDs
//        Map<String, Integer> repToGlobal = new HashMap<>();
//        int globalId = 1;
//        for (String rep : new HashSet<>(keyToRoot.values())) {
//            repToGlobal.put(rep, globalId++);
//        }
//
//        System.out.println("Number of unique clusters: " + repToGlobal.size());
//
//// Step 6: Broadcast the mappings
//        Broadcast<Map<String, String>> bcKeyToRoot = sc.broadcast(keyToRoot);
//        Broadcast<Map<String, Integer>> bcRepToGlobal = sc.broadcast(repToGlobal);
//
//// Step 7: Assign global IDs to points
//        JavaPairRDD<Double, Integer> pointToGlobal = groupedByPoint.mapValues(pts -> {
//            Map<String, String> k2r = bcKeyToRoot.value();
//            Map<String, Integer> r2g = bcRepToGlobal.value();
//
//            for (Point p : pts) {
//                if (p.clusterId > 0) {
//                    String localKey = p.cellId + "_" + p.clusterId;
//                    String rep = k2r.getOrDefault(localKey, localKey);
//                    Integer gid = r2g.get(rep);
//
//                    if (gid != null) {
//                        if (p.isCorePoint) return gid;  // Prefer core
//                    }
//                }
//            }
//
//            // Try border if no core found
//            for (Point p : pts) {
//                if (p.clusterId > 0) {
//                    String localKey = p.cellId + "_" + p.clusterId;
//                    String rep = k2r.getOrDefault(localKey, localKey);
//                    Integer gid = r2g.get(rep);
//                    if (gid != null) return gid;
//                }
//            }
//
//            return -1;
//        });
//
//// Step 8: Final output
//        JavaPairRDD<Double, Point> localOnly = dbscanClusteredAsPerCellsRDD
//                .filter(t -> t._2.isLocalRegion)
//                .mapToPair(t -> new Tuple2<>(t._2.id, t._2));
//
//        JavaRDD<Point> finalClusters = localOnly
//                .leftOuterJoin(pointToGlobal)
//                .map(t -> {
//                    Point p = t._2._1;
//                    p.clusterId = t._2._2.orElse(-1);
//                    return p;
//                });
//
//        System.out.println("Final count: " + finalClusters.count());
//        finalClusters.coalesce(1).saveAsTextFile("output/finalClusters_" + runId);


        List<Tuple2<String, String>> samePointMergeRDDList = samePointMergeRDD.collect();
        JavaPairRDD<Integer, Point> boundaryOnlyRDD = dbscanClusteredAsPerCellsRDD.filter(t -> !t._2.isLocalRegion);
        JavaPairRDD<Integer, Iterable<Point>> boundaryPointsByCellRDD = boundaryOnlyRDD.groupByKey();
        //Merge core to non core or core with different points in a cell.
//        List<Tuple2<String, String>> differentPointsMergeRDD =
//                boundaryPointsByCellRDD
//                        .flatMap(cellGroup -> {
//                            List<Point> pts = new ArrayList<>();
//                            cellGroup._2.forEach(pts::add);
//
//                            List<Tuple2<String, String>> merges = new ArrayList<>();
//
//                            for (int i = 0; i < pts.size(); i++) {
//                                for (int j = i + 1; j < pts.size(); j++) {
//
//                                    Point a = pts.get(i);
//                                    Point b = pts.get(j);
//
//                                    if (a.clusterId <= 0 || b.clusterId <= 0)
//                                        continue;
//
//                                    if (!(a.isCorePoint || b.isCorePoint))
//                                        continue;
//
//                                    double dist = distance(a, b);
//
//                                    if (dist <= eps) {
//                                        String keyA = a.cellId + "_" + a.clusterId;
//                                        String keyB = b.cellId + "_" + b.clusterId;
//
//                                        if (!keyA.equals(keyB)) {
//                                            merges.add(new Tuple2<>(keyA, keyB));
//                                        }
//                                    }
//                                }
//                            }
//
//                            return merges.iterator();
//                        })
//                        .distinct()
//                        .collect();

        UnionFindString uff = new UnionFindString();
        for (Tuple2<String, String> e : samePointMergeRDDList) {
            uff.union(e._1, e._2);
        }
//        for (Tuple2<String, String> e : differentPointsMergeRDD) {
//            uff.union(e._1, e._2);
//        }

        Map<String, String> keyToRoot = new HashMap<>();
        for (String k : uff.parent.keySet()) {
            keyToRoot.put(k, uff.find(k));
        }

         //Add isolated clusters
        for (Point p : dbscanClusteredAsPerCellsRDD.values().collect()) {
            if (p.clusterId > 0) {
                String k = p.cellId + "_" + p.clusterId;
                keyToRoot.putIfAbsent(k, k);
            }
        }

        Map<String, Integer> mapToGlobalId = new HashMap<>();
        int nnext = 1;
        for (String rep : new HashSet<>(keyToRoot.values())) {
            mapToGlobalId.put(rep, ++nnext);
        }

        Broadcast<Map<String, String>> bcKeyToRoot = sc.broadcast(keyToRoot);
        Broadcast<Map<String, Integer>> bcMapToGlobalId = sc.broadcast(mapToGlobalId);

        JavaPairRDD<Double, Integer> pointToGlobalId =
                groupedByPoint.mapValues(pts -> {

                    boolean hasCore = false;
                    boolean hasBorder = false;
                    Integer coreGid = null;
                    Integer borderGid = null;

                    for (Point p : pts) {
                        if (p.clusterId > 0) {
                            // Map local cluster -> representative -> global id
                            String key = p.cellId + "_" + p.clusterId;
                            String rep = bcKeyToRoot.value().getOrDefault(key, key);
                            Integer gid = bcMapToGlobalId.value().get(rep);
                            if (gid == null) continue;

                            if (p.isCorePoint) {
                                hasCore = true;
                                coreGid = gid;
                            } else {
                                hasBorder = true;
                                borderGid = gid;
                            }
                        }
                    }

                    if (hasCore) {
                        return coreGid;
                    }

                    if (hasBorder) {
                        return borderGid;
                    }

                    return -1;
                });


        JavaPairRDD<Double, Point> idPointPairRDD = dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2));

        JavaRDD<Point> finalClusters = idPointPairRDD.join(pointToGlobalId)
                .map(t -> {
                    Point p = t._2._1;
                    p.clusterId = t._2._2;
                    return p;
                })
                .filter(p -> p.isLocalRegion);

        System.out.println("FinalClusters count = " + finalClusters.count());
        finalClusters.coalesce(1).saveAsTextFile("output/finalClusterss"+ runId);
        long localCount = finalClusters.filter(p -> p.isLocalRegion).count();
        System.out.println("Local: " + localCount);

        long boundaryCount = dbscanClusteredAsPerCellsRDD.filter(p-> !p._2.isLocalRegion).count();
        System.out.println("Boundary: " + boundaryCount);
        long t0 = System.nanoTime();
        List<Point> result = dbscanClusteredAsPerCellsRDD.values().collect();
        long t1 = System.nanoTime();
        System.out.println("Collected size: " + result.size());
        System.out.println("DBSCAN time: " + (t1 - t0) / 1e9);
        System.out.println("Neighbor queries: " + neighborQueryCount);

        sc.close();
    }



    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId) {
        Point ghost = new Point(original.id, original.latitude, original.longitude, 0);
        ghost.cellId = targetCellId;
        ghost.isLocalRegion = false;
        list.add(new Tuple2<>(targetCellId, ghost));
    }



    public static double distance(Point a, Point b) {
        double dx = a.latitude - b.latitude;
        double dy = a.longitude - b.longitude;
        return Math.sqrt(dx * dx + dy * dy);
    }


    public static void localDBSCAN(List<Point> points, double eps, int minPts,LongAccumulator queryCount, LongAccumulator queryTime) {

        Map<Double, Boolean> visited = new HashMap<>();
        int localClusterId = 0;

        for (Point p : points) {
            if (visited.getOrDefault(p.id, false)) {
                continue;
            }

            visited.put(p.id, true);
            List<Point> neighbors = regionQuery(points, p, eps, queryCount, queryTime);

            if (neighbors.size() < minPts) {
                p.clusterId = -1;
                p.isCorePoint = false;
            } else {
                p.isCorePoint = true;
                localClusterId++;
                expandCluster(points, p, neighbors, localClusterId, eps, minPts, visited, queryCount, queryTime);
            }
        }
    }

    public static void expandCluster(List<Point> points, Point p, List<Point> neighbors,
                                     int clusterId, double eps, int minPts, Map<Double, Boolean> visited,
                                     LongAccumulator queryCount, LongAccumulator queryTime) {
        p.clusterId = clusterId;

        Queue<Point> seeds = new LinkedList<>(neighbors);

        while (!seeds.isEmpty()) {
            Point q = seeds.poll();

            if (!visited.getOrDefault(q.id, false)) {
                visited.put(q.id, true);
                List<Point> qNeighbors = regionQuery(points, q, eps, queryCount, queryTime);

                if (qNeighbors.size() >= minPts) {
                    q.isCorePoint = true;
                    seeds.addAll(qNeighbors);
                }
            }

            if (q.clusterId <= 0) {
                q.clusterId = clusterId;
            }
        }
    }


    public static List<Point> regionQuery(List<Point> points, Point p, double eps, LongAccumulator queryCount, LongAccumulator queryTime) {
        long start = System.nanoTime();

        List<Point> neighbors = new ArrayList<>();
        for (Point q : points) {
            if (distance(p, q) <= eps) {
                neighbors.add(q);
            }
        }

        long end = System.nanoTime();
        queryTime.add(end - start);
        queryCount.add(1);

        return neighbors;
    }


    static class UnionFindString {
        Map<String, String> parent = new HashMap<>();

        String find(String x) {
            parent.putIfAbsent(x, x);
            if (!parent.get(x).equals(x)) {
                parent.put(x, find(parent.get(x)));
            }
            return parent.get(x);
        }

        void union(String a, String b) {
            String pa = find(a);
            String pb = find(b);
            if (!pa.equals(pb)) parent.put(pa, pb);
        }
    }
}




