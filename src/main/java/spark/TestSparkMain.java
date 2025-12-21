package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

import static org.spark_project.guava.primitives.Doubles.max;
import static org.spark_project.guava.primitives.Doubles.min;

public class TestSparkMain {
    static long neighborQueryTimeNs = 0;
    static long neighborQueryCount = 0;
    static double eps=0.03;
    static  int minPts=50;
    static int numCellsX,numCellsY;
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DBSCAN Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("src/main/resources/densired_2_shrink.csv");
        //JavaRDD<String> lines = sc.textFile("src/main/resources/test.txt");
        JavaRDD<String> nonEmptyLines=lines.filter(s -> !s.trim().isEmpty());
        JavaRDD<Point> points = nonEmptyLines
                .map(TestSparkMain::parsePoint)
                .filter(p -> p != null);

        // Find min/max coordinates of entire dataset
        double minLatitude = points.map(p->p.latitude).reduce(Double::min);
        double maxLatitude = points.map(p -> p.latitude).reduce(Double::max);
        double minLongitude = points.map(p -> p.longitude).reduce(Double::min);
        double maxLongitude = points.map(p -> p.longitude).reduce(Double::max);
        System.out.println("minLatitude: " + minLatitude+" maxLatitude: " + maxLatitude +" minLongitude: " + minLongitude +" maxLongitude: " + maxLongitude);
        double cellSize = 3 * eps; // Paper uses 3Eps
        // Calculate number of cells in X and Y directions

        TestSparkMain.numCellsX = (int) Math.ceil((maxLatitude - minLatitude) / cellSize);
        TestSparkMain.numCellsY = (int) Math.ceil((maxLongitude - minLongitude) / cellSize);

        // For each point, get all cells it belongs to (including expanded cells)
        JavaPairRDD<Integer, Point> expandedAssignments = points.flatMapToPair(
                point -> {
                    List<Integer> cellIds = getExpandedCellIds(point, eps, minLatitude, minLongitude,
                            cellSize, numCellsX, numCellsY);
                    List<Tuple2<Integer, Point>> assignments = new ArrayList<>();
                    for (Integer cellId : cellIds) {
                        int cellX = cellId % numCellsX;
                        int cellY = cellId / numCellsX;

                        Point copy = new Point(point.latitude, point.longitude, 0);
                        copy.cellId = cellId;

                        copy.isLocalRegion = isLocalRegion(
                                point, cellX, cellY,
                                minLatitude, minLongitude,
                                cellSize, eps
                        );

                        assignments.add(new Tuple2<>(cellId, copy));

                    }
                    return assignments.iterator();
                }
        );
        JavaPairRDD<Integer, Iterable<Point>> partitions =
                expandedAssignments.groupByKey();


        JavaRDD<Point> clustered =
                partitions.flatMap(cell -> {

                    List<Point> cellPoints = new ArrayList<>();
                    cell._2.forEach(cellPoints::add);

                    // run DBSCAN ONLY on this cell
                    List<Point> clusteredCell =
                            localDBSCAN(cellPoints, eps, minPts);

                    return clusteredCell.iterator();
                });


        JavaPairRDD<String, Iterable<Point>> groupedByPoint =
                clustered.mapToPair(p ->
                        new Tuple2<>(p.latitude + "," + p.longitude, p)
                ).groupByKey();


        JavaPairRDD<String, String> mergePairs =
                groupedByPoint.flatMapToPair(entry -> {

                    Set<String> clusterKeys = new HashSet<>();

                    for (Point p : entry._2) {
                        if (!p.isLocalRegion && p.isCorePoint && p.clusterId > 0) {
                            clusterKeys.add(p.cellId + "_" + p.clusterId);

                        }
                    }

                    List<Tuple2<String, String>> pairs = new ArrayList<>();
                    List<String> ids = new ArrayList<>(clusterKeys);

                    for (int i = 0; i < ids.size(); i++) {
                        for (int j = i + 1; j < ids.size(); j++) {
                            pairs.add(new Tuple2<>(ids.get(i), ids.get(j)));
                        }
                    }
                    return pairs.iterator();
                });
        List<Tuple2<String, String>> mergeList = mergePairs.collect();

        UnionFindString uf = new UnionFindString();
        for (Tuple2<String, String> pair : mergeList) {
            uf.union(pair._1, pair._2);
        }

        Map<String, String> keyToRep = new HashMap<>();
        for (String k : uf.parent.keySet()) {
            keyToRep.put(k, uf.find(k));
        }

        // Add isolated clusters (never merged)
        for (Point p : clustered.collect()) {
            if (p.clusterId > 0) {
                String key = p.cellId + "_" + p.clusterId;
                if (!keyToRep.containsKey(key)) {
                    keyToRep.put(key, key);
                }
            }
        }

        Broadcast<Map<String, String>> bcKeyToRep = sc.broadcast(keyToRep);


        Map<String, Integer> repToGlobalId = new HashMap<>();
        int next = 1;
        for (String rep : new HashSet<>(keyToRep.values())) {
            repToGlobalId.put(rep, next++);
        }

        Broadcast<Map<String, Integer>> bcRepToGlobalId = sc.broadcast(repToGlobalId);


        JavaRDD<Point> finalClusters =
                clustered.map(p -> {
                    if (p.clusterId > 0) {
                        String key = p.cellId + "_" + p.clusterId;

                        String rep = bcKeyToRep.value().getOrDefault(key, key);

                        int gid = bcRepToGlobalId.value().getOrDefault(rep, 0);

                        p.clusterId = gid;
                    }
                    return p;
                });


        JavaRDD<Point> output = finalClusters.filter(p -> p.isLocalRegion);

        finalClusters.foreach(p -> {System.out.println(p.latitude + "," + p.longitude + ", " + p.clusterId);});
        finalClusters.map(Point::toString).coalesce(1).saveAsTextFile("output/dbscan_result_single");

        long localCount =
                clustered.filter(p -> p.isLocalRegion).count();

        long boundaryCount =
                clustered.filter(p -> !p.isLocalRegion).count();

        System.out.println("Local: " + localCount);
        System.out.println("Boundary: " + boundaryCount);




        //expandedAssignments.collect().forEach(System.out::println);

        //Without any partition call local DBSCAN
//        JavaPairRDD<Point,Integer> initialClusters=points.mapToPair(point->new Tuple2<>(point,0));
//        JavaRDD<Point> clustered = points.mapPartitions(iter -> {
//            List<Point> localPoints = new ArrayList<>();
//            iter.forEachRemaining(localPoints::add);
//
//            List<Point> result = localDBSCAN(
//                    localPoints,
//                    eps ,
//                    minPts
//            );
//
//            return result.iterator();
//        });

        long t0 = System.nanoTime();
        List<Point> result = clustered.collect();
        long t1 = System.nanoTime();

        System.out.println("Collected size: " + result.size());
        //JavaRDD<String> output = clustered.map(Point::toString);
        //output.saveAsTextFile("output/dbscan_result");

        //clustered.map(Point::toString).coalesce(1).saveAsTextFile("output/dbscan_result_single");

        System.out.println("DBSCAN time: " + (t1 - t0)/1e9);
        System.out.println("Neighbor queries: " + neighborQueryCount);
        System.out.println("Total neighbor query time (s): " + neighborQueryTimeNs / 1e9);
        System.out.println("Avg time per query (ms): " + (neighborQueryTimeNs / 1e6) / neighborQueryCount);

       // sc.close();
    }


    public static List<Integer> getExpandedCellIds(Point p, double eps,
                                            double minX, double minY,
                                            double cellSize,
                                            int numCellsX, int numCellsY) {

        List<Integer> cellIds = new ArrayList<>();

        // Original cell
        int origCellX = (int) Math.floor((p.latitude - minX) / cellSize);
        int origCellY = (int) Math.floor((p.longitude - minY) / cellSize);
        cellIds.add(origCellY * numCellsX + origCellX);

        // Check if point is within 0.1Eps of cell boundaries
        double buffer = 0.1 * eps;

        // Calculate distance to cell boundaries
        double leftBoundary = minX + origCellX * cellSize;
        double rightBoundary = leftBoundary + cellSize;
        double bottomBoundary = minY + origCellY * cellSize;
        double topBoundary = bottomBoundary + cellSize;

        // If close to left boundary, include left cell
        if (p.latitude - leftBoundary < buffer && origCellX > 0) {
            cellIds.add(origCellY * numCellsX + (origCellX - 1));
        }

        // If close to right boundary, include right cell
        if (rightBoundary - p.latitude < buffer && origCellX < numCellsX - 1) {
            cellIds.add(origCellY * numCellsX + (origCellX + 1));
        }

        // If close to bottom boundary, include bottom cell
        if (p.longitude - bottomBoundary < buffer && origCellY > 0) {
            cellIds.add((origCellY - 1) * numCellsX + origCellX);
        }

        // If close to top boundary, include top cell
        if (topBoundary - p.longitude < buffer && origCellY < numCellsY - 1) {
            cellIds.add((origCellY + 1) * numCellsX + origCellX);
        }

        return cellIds;
    }

    public static boolean isLocalRegion(
            Point p,
            int cellX,
            int cellY,
            double minLat,
            double minLon,
            double cellSize,
            double eps
    ) {
        double expand = 0.1 * eps;

        double cellMinLat = minLat + cellX * cellSize;
        double cellMaxLat = cellMinLat + cellSize;
        double cellMinLon = minLon + cellY * cellSize;
        double cellMaxLon = cellMinLon + cellSize;

        return p.latitude > cellMinLat + expand &&
                p.latitude < cellMaxLat - expand &&
                p.longitude > cellMinLon + expand &&
                p.longitude < cellMaxLon - expand;
    }


    private static Point parsePoint(String line) {
        if (line.trim().isEmpty()) {
            return null;
        }
        String[] fields = line.trim().split(",");
        if (fields.length < 2) {
            return null;
        }
        return new Point(Double.parseDouble(fields[0]),
                Double.parseDouble(fields[1]),0);
    }




    public static double distance(Point a, Point b) {
        double dx = a.latitude - b.latitude;
        double dy = a.longitude - b.longitude;
        return Math.sqrt(dx * dx + dy * dy);
    }


    public static List<Point> localDBSCAN(List<Point> points, double eps, int minPts) {
        int clusterId = 0;
        Map<Point, Boolean> visited = new HashMap<>();

        for (Point p : points) {
            if (visited.getOrDefault(p, false)) {
                continue;
            }

            visited.put(p, true);
            List<Point> neighbors = regionQuery(points, p, eps);

            if (neighbors.size() < minPts) {
                p.clusterId = -1; // noise
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




