package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.coalesce;

public class TestSparkMain {
    static long neighborQueryTimeNs = 0;
    static long neighborQueryCount = 0;
    static double eps = 0.03;
    static int minPts = 50;
    static int clusterId = 0;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DBSCAN Analysis").setMaster("local[*]").set("spark.driver.maxResultSize", "4g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rawLines = sc.textFile("src/main/resources/densired_2_shrink.csv");
        //JavaRDD<String> rawLines = sc.textFile("src/main/resources/test.txt");
        JavaRDD<String> nonEmptyLines = rawLines.filter(s -> !s.trim().isEmpty());
        JavaRDD<Point> points = nonEmptyLines
                .map(TestSparkMain::parsePoint)
                .filter(p -> p != null);


        JavaRDD<Point> pointt = rawLines.zipWithIndex()
                .filter(t -> !t._1.trim().isEmpty()).map(t -> {
                    String[] parts = t._1.trim().split(",");
                    double x = Double.parseDouble(parts[0]);
                    double y = Double.parseDouble(parts[1]);
                    return new Point(t._2, x, y, 0);
                }).cache();


        // Find min/max coordinates of entire dataset
        double minLatitude = points.map(p -> p.latitude).reduce(Double::min);
        double maxLatitude = points.map(p -> p.latitude).reduce(Double::max);
        double minLongitude = points.map(p -> p.longitude).reduce(Double::min);
        double maxLongitude = points.map(p -> p.longitude).reduce(Double::max);

        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(minLatitude, maxLatitude, minLongitude, maxLongitude, eps);
        double cellSize = partitionConfiguration.cellSize;
        int numCellsX = partitionConfiguration.numCellsX;
        int numCellsY = partitionConfiguration.numCellsY;
        double buffer = partitionConfiguration.buffer;

        System.out.println("minLatitude: " + partitionConfiguration.minLatitude + " maxLatitude: " + partitionConfiguration.maxLatitude + " minLongitude: " + partitionConfiguration.minLongitude
                + " maxLongitude: " + partitionConfiguration.maxLongitude + " buffer: " + buffer + " numCellsX: " + numCellsX + " numCellsY: " + numCellsY + " cellSize: " + cellSize);

        JavaPairRDD<Integer, Point> partitionedToCellsRDD = pointt.flatMapToPair(p -> {
            List<Tuple2<Integer, Point>> assignments = new ArrayList<>();
            //Determine Home Cell (Geometric Location)
            int homeX = (int) Math.floor((p.latitude - minLatitude) / cellSize);
            int homeY = (int) Math.floor((p.longitude - minLongitude) / cellSize);
            // Fix to safe bounds
            homeX = Math.max(0, Math.min(homeX, numCellsX - 1));
            homeY = Math.max(0, Math.min(homeY, numCellsY - 1));
            int homeCellId = homeY * numCellsX + homeX;

            // Add to Home Cell (Always Local)
            Point local = new Point(p.id, p.latitude, p.longitude, 0);
            local.cellId = homeCellId;
            local.isLocalRegion = true;
            assignments.add(new Tuple2<>(homeCellId, local));

            // Check Boundaries for Neighbor Replication (Ghost Points)
            double cellMinX = minLatitude + homeX * cellSize;
            double cellMinY = minLongitude + homeY * cellSize;

            double dxLeft = p.latitude - cellMinX;
            double dxRight = (cellMinX + cellSize) - p.latitude;
            double dyBottom = p.longitude - cellMinY;
            double dyTop = (cellMinY + cellSize) - p.longitude;


            // Left Neighbor
            if (homeX > 0 && dxLeft <= buffer) {
                addGhost(assignments, p, homeY * numCellsX + (homeX - 1));
            }
            // Right Neighbor
            if (homeX < numCellsX - 1 && dxRight <= buffer) {
                addGhost(assignments, p, homeY * numCellsX + (homeX + 1));
            }
            // Bottom Neighbor
            if (homeY > 0 && dyBottom <= buffer) {
                addGhost(assignments, p, (homeY - 1) * numCellsX + homeX);
            }
            // Top Neighbor
            if (homeY < numCellsY - 1 && dyTop <= buffer) {
                addGhost(assignments, p, (homeY + 1) * numCellsX + homeX);
            }

            // Top-Left (homeX-1, homeY+1)
            if (homeX > 0 && homeY < numCellsY - 1 && dxLeft <= buffer && dyTop <= buffer) {
                int cellId = (homeY + 1) * numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId);
            }

            // Top-Right (homeX+1, homeY+1)
            if (homeX < numCellsX - 1 && homeY < numCellsY - 1 && dxRight <= buffer && dyTop <= buffer) {
                int cellId = (homeY + 1) * numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId);
            }

            // Bottom-Left (homeX-1, homeY-1)
            if (homeX > 0 && homeY > 0 && dxLeft <= buffer && dyBottom <= buffer) {
                int cellId = (homeY - 1) * numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId);
            }

            // Bottom-Right (homeX+1, homeY-1)
            if (homeX < numCellsX - 1 && homeY > 0 && dxRight <= buffer && dyBottom <= buffer) {
                int cellId = (homeY - 1) * numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId);
            }
            return assignments.iterator();
        });
        partitionedToCellsRDD.saveAsTextFile("output/partitionedToCells");



//        JavaRDD<Point> clusteredAsPerCell = partitionedToCells.groupByKey().flatMap(cell -> {
//            List<Point> cellPoints = new ArrayList<>();
//            cell._2.forEach(cellPoints::add);
//            localDBSCAN(cellPoints, eps, minPts);
//            return cellPoints.iterator();
//        }).cache();

        JavaPairRDD<Integer, Point> dbscanClusteredAsPerCellsRDD =
                partitionedToCellsRDD
                        .groupByKey()
                        .flatMapToPair(cell -> {

                            int cellId = cell._1;
                            List<Point> cellPoints = new ArrayList<>();
                            cell._2.forEach(cellPoints::add);

                            localDBSCAN(cellPoints, eps, minPts);

                            List<Tuple2<Integer, Point>> out = new ArrayList<>();
                            for (Point p : cellPoints) {
                                out.add(new Tuple2<>(cellId, p));
                            }
                            return out.iterator();
                        })
                        .cache();
        dbscanClusteredAsPerCellsRDD.saveAsTextFile("output/dbscanClusteredAsPerCells");

        JavaPairRDD<Double, Iterable<Point>> groupedByPoint =
                dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2))
                        .groupByKey();

//            Boundary core points merge process
//        JavaPairRDD<String, String> mergePairrs =
//                groupedByPoint.flatMapToPair(entry -> {
//
//                    Set<String> clusterKeys = new HashSet<>();
//
//                    for (Point p : entry._2) {
//                        if (p.clusterId > 0 && !p.isLocalRegion && p.isCorePoint) {
//                            clusterKeys.add(p.cellId + "_" + p.clusterId);
//                           // System.out.println("Cluster keys: "+p.cellId + "_" + p.clusterId+", "+p.id);
//                        }
//                    }
//
//                    List<Tuple2<String, String>> pairs = new ArrayList<>();
//                    List<String> ids = new ArrayList<>(clusterKeys);
//
//                    for (int i = 0; i < ids.size(); i++) {
//                        for (int j = i + 1; j < ids.size(); j++) {
//                            pairs.add(new Tuple2<>(ids.get(i), ids.get(j)));
//                        }
//                    }
//
//                    return pairs.iterator();
//                });

        //Creating merge list for same point either core or non core
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

                                if (!keyA.equals(keyB)) {
                                    merges.add(new Tuple2<>(keyA, keyB));
                                }
                            }
                        }
                    }

                    return merges.iterator();
                });


        samePointMergeRDD.saveAsTextFile("output/samePointMergeRDD");
        List<Tuple2<String, String>> samePointMergeRDDList = samePointMergeRDD.collect();
        JavaPairRDD<Integer, Point> boundaryOnlyRDD = dbscanClusteredAsPerCellsRDD.filter(t -> !t._2.isLocalRegion);
        JavaPairRDD<Integer, Iterable<Point>> boundaryPointsByCellRDD = boundaryOnlyRDD.groupByKey();

        //Merge core to non core or core with different points in a cell.
        List<Tuple2<String, String>> differentPointsMergeRDD =
                boundaryPointsByCellRDD
                        .flatMap(cellGroup -> {
                            List<Point> pts = new ArrayList<>();
                            cellGroup._2.forEach(pts::add);

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

                                        if (!keyA.equals(keyB)) {
                                            merges.add(new Tuple2<>(keyA, keyB));
                                        }
                                    }
                                }
                            }

                            return merges.iterator();
                        })
                        .distinct()
                        .collect();

        UnionFindString uff = new UnionFindString();
        for (Tuple2<String, String> e : samePointMergeRDDList) {
            uff.union(e._1, e._2);
        }
        for (Tuple2<String, String> e : differentPointsMergeRDD) {
            uff.union(e._1, e._2);
        }

        Map<String, String> keyToRoot = new HashMap<>();
        for (String k : uff.parent.keySet()) {
            keyToRoot.put(k, uff.find(k));
        }

        // Add isolated clusters
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


        JavaPairRDD<Double, Point> idPointPairRDD =
                dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2));

        JavaRDD<Point> finalClusters = idPointPairRDD.join(pointToGlobalId)
                .map(t -> {
                    Point p = t._2._1;
                    p.clusterId = t._2._2;
                    return p;
                })
                .filter(p -> p.isLocalRegion);

        finalClusters.coalesce(1).saveAsTextFile("output/finalClusters");

        long localCount = finalClusters.filter(p -> p.isLocalRegion).count();
        long boundaryCount = dbscanClusteredAsPerCellsRDD.filter(p-> !p._2.isLocalRegion).count();
        System.out.println("Local: " + localCount);
        System.out.println("Boundary: " + boundaryCount);
        long t0 = System.nanoTime();
        List<Point> result = dbscanClusteredAsPerCellsRDD.values().collect();
        long t1 = System.nanoTime();
        System.out.println("Collected size: " + result.size());
        System.out.println("DBSCAN time: " + (t1 - t0) / 1e9);
        System.out.println("Neighbor queries: " + neighborQueryCount);
        System.out.println("Total neighbor query time (s): " + neighborQueryTimeNs / 1e9);
        System.out.println("Avg time per query (ms): " + (neighborQueryTimeNs / 1e6) / neighborQueryCount);

        sc.close();
    }


    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId) {
        Point ghost = new Point(original.id, original.latitude, original.longitude, 0);
        ghost.cellId = targetCellId;
        ghost.isLocalRegion = false;
        list.add(new Tuple2<>(targetCellId, ghost));
    }

    private static Point parsePoint(String line) {
        if (line.trim().isEmpty()) {
            return null;
        }
        String[] fields = line.trim().split(",");
        if (fields.length < 2) {
            return null;
        }
        return new Point(11, Double.parseDouble(fields[0]),
                Double.parseDouble(fields[1]), 0);
    }


    public static double distance(Point a, Point b) {
        double dx = a.latitude - b.latitude;
        double dy = a.longitude - b.longitude;
        return Math.sqrt(dx * dx + dy * dy);
    }


    public static List<Point> localDBSCAN(List<Point> points, double eps, int minPts) {
        Map<Point, Boolean> visited = new HashMap<>();

        for (Point p : points) {
            if (visited.getOrDefault(p, false)) {
                continue;
            }

            visited.put(p, true);
            List<Point> neighbors = regionQuery(points, p, eps);

            if (neighbors.size() < minPts) {
                p.clusterId = -1;
                p.isCorePoint = false;
            } else {
                p.isCorePoint = true;
                clusterId++;
                expandCluster(points, p, neighbors, clusterId, eps, minPts, visited);
            }
        }
        return points;
    }

    public static void expandCluster(List<Point> points, Point p, List<Point> neighbors, int clusterId, double eps, int minPts,
                                     Map<Point, Boolean> visited) {
        p.clusterId = clusterId;

        Queue<Point> seeds = new LinkedList<>(neighbors);

        while (!seeds.isEmpty()) {
            Point q = seeds.poll();

            if (!visited.getOrDefault(q, false)) {
                visited.put(q, true);
                List<Point> qNeighbors = regionQuery(points, q, eps);

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


    public static List<Point> regionQuery(List<Point> points, Point p, double eps) {
        long start = System.nanoTime();

        List<Point> neighbors = new ArrayList<>();
        for (Point q : points) {
            if (distance(p, q) <= eps) {
                neighbors.add(q);
            }
        }

        long end = System.nanoTime();

        neighborQueryTimeNs += (end - start);
        neighborQueryCount++;

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




