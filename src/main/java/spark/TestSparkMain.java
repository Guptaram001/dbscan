package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.expressions.Lit;
import org.codehaus.janino.Java;
import scala.Tuple2;

import javax.servlet.http.Part;
import java.util.*;

import static org.apache.spark.sql.functions.coalesce;

public class TestSparkMain {
    static long neighborQueryTimeNs = 0;
    static long neighborQueryCount = 0;
    static double eps=0.03;
    static  int minPts=50;
    static int clusterId=0;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DBSCAN Analysis").setMaster("local[*]").set("spark.driver.maxResultSize", "4g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rawLines = sc.textFile("src/main/resources/densired_2.csv");
        //JavaRDD<String> rawLines = sc.textFile("src/main/resources/test.txt");
        JavaRDD<String> nonEmptyLines=rawLines.filter(s -> !s.trim().isEmpty());
        JavaRDD<Point> points = nonEmptyLines
                .map(TestSparkMain::parsePoint)
                .filter(p -> p != null);


        JavaRDD<Point> pointt = rawLines.zipWithIndex()
                .filter(t -> !t._1.trim().isEmpty()).map(t -> {
                    String[] parts = t._1.trim().split(",");
                    double x = Double.parseDouble(parts[0]);
                    double y = Double.parseDouble(parts[1]);
                    return new Point(t._2, x, y,0);
                }).cache();


        // Find min/max coordinates of entire dataset
        double minLatitude = points.map(p->p.latitude).reduce(Double::min);
        double maxLatitude = points.map(p -> p.latitude).reduce(Double::max);
        double minLongitude = points.map(p -> p.longitude).reduce(Double::min);
        double maxLongitude = points.map(p -> p.longitude).reduce(Double::max);

        PartitionConfiguration partitionConfiguration=new PartitionConfiguration(minLatitude,maxLatitude,minLongitude,maxLongitude,eps);
        double  cellSize = partitionConfiguration.cellSize;
        int numCellsX = partitionConfiguration.numCellsX;
        int numCellsY = partitionConfiguration.numCellsY;
        double buffer=partitionConfiguration.buffer;
        System.out.println("minLatitude: " + partitionConfiguration.minLatitude+" maxLatitude: " + partitionConfiguration.maxLatitude +" minLongitude: " + partitionConfiguration.minLongitude
                +" maxLongitude: " + partitionConfiguration.maxLongitude+" buffer: " + buffer+" numCellsX: "+numCellsX+" numCellsY: "+numCellsY +" cellSize: "+cellSize);

        JavaPairRDD<Integer, Point> partitionedToCells = pointt.flatMapToPair(p -> {
            List<Tuple2<Integer, Point>> assignments = new ArrayList<>();

            // A. Determine Home Cell (Geometric Location)
            int homeX = (int) Math.floor((p.latitude - minLatitude) / cellSize);
            int homeY = (int) Math.floor((p.longitude - minLongitude) / cellSize);
            // Clamp to safe bounds
            homeX = Math.max(0, Math.min(homeX, numCellsX - 1));
            homeY = Math.max(0, Math.min(homeY, numCellsY - 1));

            int homeCellId = homeY * numCellsX + homeX;

            // B. Add to Home Cell (Always Local)
            Point localCopy = new Point(p.id, p.latitude, p.longitude,0);
            localCopy.cellId = homeCellId;
            localCopy.isLocalRegion = true; // [cite: 316]
            assignments.add(new Tuple2<>(homeCellId, localCopy));

            // C. Check Boundaries for Neighbor Replication (Ghost Points)
            // Paper Logic: Replicate if within 0.1 Eps of boundary
            double cellMinX = minLatitude + homeX * cellSize;
            double cellMinY = minLongitude + homeY * cellSize;

            double dxLeft   = p.latitude  - cellMinX;
            double dxRight  = (cellMinX + cellSize) - p.latitude;
            double dyBottom = p.longitude - cellMinY;
            double dyTop    = (cellMinY + cellSize) - p.longitude;


            // Left Neighbor
            if (homeX > 0 && (p.latitude - cellMinX) <= buffer) {
                addGhost(assignments, p, homeY * numCellsX + (homeX - 1));
            }
            // Right Neighbor
            if (homeX < numCellsX - 1 && (cellMinX + cellSize - p.latitude) <= buffer) {
                addGhost(assignments, p, homeY * numCellsX + (homeX + 1));
            }
            // Bottom Neighbor
            if (homeY > 0 && (p.longitude - cellMinY) <= buffer) {
                addGhost(assignments, p, (homeY - 1) * numCellsX + homeX);
            }
            // Top Neighbor
            if (homeY < numCellsY - 1 && (cellMinY + cellSize - p.longitude) <= buffer) {
                addGhost(assignments, p, (homeY + 1) * numCellsX + homeX);
            }

            // Note: Corner neighbors (diagonals) can be added similarly if strict Euclidean accuracy
            // is needed at corners, but standard strip implementation usually suffices for 3*Eps grids.

            // Top-Left (homeX-1, homeY+1)
            if (homeX > 0 && homeY < numCellsY - 1 &&
                    dxLeft <= buffer && dyTop <= buffer) {
                int cellId = (homeY + 1) * numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId);
            }

// Top-Right (homeX+1, homeY+1)
            if (homeX < numCellsX - 1 && homeY < numCellsY - 1 &&
                    dxRight <= buffer && dyTop <= buffer) {
                int cellId = (homeY + 1) * numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId);
            }

// Bottom-Left (homeX-1, homeY-1)
            if (homeX > 0 && homeY > 0 &&
                    dxLeft <= buffer && dyBottom <= buffer) {
                int cellId = (homeY - 1) * numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId);
            }

// Bottom-Right (homeX+1, homeY-1)
            if (homeX < numCellsX - 1 && homeY > 0 &&
                    dxRight <= buffer && dyBottom <= buffer) {
                int cellId = (homeY - 1) * numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId);
            }


            return assignments.iterator();
        });

        partitionedToCells.saveAsTextFile("output/partitionedToCells");

        JavaRDD<Point> clusteredd = partitionedToCells.groupByKey().flatMap(cell -> {
            List<Point> cellPoints = new ArrayList<>();
            cell._2.forEach(cellPoints::add);
            localDBSCAN(cellPoints, eps, minPts);
            return cellPoints.iterator();
        }).cache();

        JavaPairRDD<Integer, Point> dbscanClusteredAsPerCells =
                partitionedToCells
                        .groupByKey()
                        .flatMapToPair(cell -> {

                            int cellId = cell._1;
                            List<Point> cellPoints = new ArrayList<>();
                            cell._2.forEach(cellPoints::add);

                            // Run DBSCAN locally inside the cell
                            localDBSCAN(cellPoints, eps, minPts);

                            // Emit (cellId, point) for each processed point
                            List<Tuple2<Integer, Point>> out = new ArrayList<>();
                            for (Point p : cellPoints) {
                                out.add(new Tuple2<>(cellId, p));
                            }

                            return out.iterator();
                        })
                        .cache();



        //partitionedToCells.collect().forEach(System.out::println);
        dbscanClusteredAsPerCells.saveAsTextFile("output/dbscanClusteredAsPerCells");
        //clusteredd.saveAsTextFile("output/clustered");


        JavaRDD<Point> boundaryPointsRDD =
                dbscanClusteredAsPerCells
                        .values()
                        .filter(p -> !p.isLocalRegion);

        JavaRDD<String> boundaryClusterKeys =
                boundaryPointsRDD
                        .map(p -> p.cellId + "_" + p.clusterId);

        List<String> boundaryPoints = boundaryClusterKeys.distinct().collect();
        boundaryPoints.forEach(p->System.out.println(p));
        boundaryPointsRDD.collect().forEach(p->System.out.println(p));


        boundaryPointsRDD.saveAsTextFile("output/boundaryPoints");







        JavaPairRDD<Double,Point> idPointPair=clusteredd.flatMapToPair(p->{
            List<Tuple2<Double, Point>> assignments = new ArrayList<>();
            assignments.add(new Tuple2<>(p.id,p));
            return assignments.iterator();
                });

        idPointPair.saveAsTextFile("output/idPointPair");

        JavaPairRDD<Double, Point> pointpaar =
                idPointPair
                        .groupByKey()
                        .flatMapToPair(cell -> {

                            List<Tuple2<Double, Point>> out = new ArrayList<>();

                            for (Point p : cell._2) {
                                out.add(new Tuple2<>(cell._1, p));
                            }

                            return out.iterator();
                        })
                        .cache();

        JavaPairRDD<Double, Iterable<Point>> groupedByPoint =
                clusteredd.mapToPair(p -> new Tuple2<>(p.id, p))
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
        JavaPairRDD<String, String> mergePairrs =
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


    mergePairrs.saveAsTextFile("output/mergePairrs");

        List<Tuple2<String, String>> mergeList = mergePairrs.collect();

        JavaPairRDD<Integer, Point> boundaryOnly =
                dbscanClusteredAsPerCells
                        .filter(p -> !p._2.isLocalRegion);



        JavaPairRDD<Integer, Iterable<Point>> boundaryPointsByCell =
                boundaryOnly.groupByKey();

        List<Tuple2<String, String>> mergePairsDif =
                boundaryPointsByCell
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
        for (Tuple2<String, String> e : mergePairsDif) {
            uff.union(e._1, e._2);
        }
        for (Tuple2<String, String> e : mergeList) {
            uff.union(e._1, e._2);
        }

        Map<String, String> keyToReepe = new HashMap<>();
        for (String k : uff.parent.keySet()) {
            keyToReepe.put(k, uff.find(k));
        }

        for(String key : keyToReepe.keySet()) {
            System.out.println("key rep: "+keyToReepe.get(key));
        }

// Add isolated clusters
        for (Point p : clusteredd.collect()) {
            if (p.clusterId > 0) {
                String k = p.cellId + "_" + p.clusterId;
                keyToReepe.putIfAbsent(k, k);
            }
        }

        Map<String, Integer> rrepToGlobalIid = new HashMap<>();
        int nnext = clusterId;
        for (String rep : new HashSet<>(keyToReepe.values())) {
            rrepToGlobalIid.put(rep, ++nnext);
        }


        Broadcast<Map<String, String>> bbcKeyToReep = sc.broadcast(keyToReepe);
        Broadcast<Map<String, Integer>> bbcRepToIid = sc.broadcast(rrepToGlobalIid);

        JavaPairRDD<Double, Iterable<Point>> byPointFinal =
                clusteredd.mapToPair(p -> new Tuple2<>(p.id, p))
                        .groupByKey();

        JavaPairRDD<Double, Integer> pointToGlobalId =
                byPointFinal.mapValues(poits -> {

                    boolean hasCore   = false;
                    boolean hasBorder = false;
                    Integer coreGid   = null;
                    Integer borderGid = null;

                    for (Point p : poits) {
                        if (p.clusterId > 0) { // local cluster member, not noise

                            // Map local cluster -> representative -> global id
                            String key = p.cellId + "_" + p.clusterId;
                            String rep = bbcKeyToReep.value().getOrDefault(key, key);
                            Integer gid = bbcRepToIid.value().get(rep);
                            if (gid == null) continue;

                            if (p.isCorePoint) {
                                hasCore = true;
                                coreGid = gid;   // Scenario 1 & 2: core wins
                            } else {
                                hasBorder = true;
                                borderGid = gid; // Scenario 3: border
                            }
                        }
                    }

                    // Scenario 1 & 2: Core in at least one cluster
                    if (hasCore) {
                        return coreGid;
                    }

                    // Scenario 3: No core, but border in some cluster(s)
                    if (hasBorder) {
                        return borderGid;
                    }

                    // Scenario 4: Noise in every cell
                    return -1;  // or 0, whichever you use for noise
                });

        JavaPairRDD<Double, Point> withPointKey =
                clusteredd.mapToPair(p -> new Tuple2<>(p.id, p));

        JavaRDD<Point> fffinalClusters =
                withPointKey.join(pointToGlobalId)
                        .map(t -> {
                            Point p = t._2._1;
                            Integer gid = t._2._2;
                            p.clusterId = gid;
                            return p;
                        });

        fffinalClusters.coalesce(1).saveAsTextFile("output/fffinalClusters");


        //finalClusters.foreach(p -> {System.out.println(p.latitude + "," + p.longitude + ", " + p.clusterId);});
        //JavaRDD<Point> finalOutput = finalClusters.filter(p -> p.isLocalRegion);

        //finalOutput.map(Point::toString).coalesce(1).saveAsTextFile("output/dbscan_result_single");

        long localCount = clusteredd.filter(p -> p.isLocalRegion).count();

        long boundaryCount = clusteredd.filter(p -> !p.isLocalRegion).count();

        System.out.println("Local: " + localCount);
        System.out.println("Boundary: " + boundaryCount);

        long t0 = System.nanoTime();
        List<Point> result = clusteredd.collect();
        long t1 = System.nanoTime();

        // No new cores after merge

        System.out.println("Clustered Filter: " + clusteredd.filter(p -> p.isCorePoint && p.clusterId > 0).count());
        System.out.println("Collected size: " + result.size());
        System.out.println("DBSCAN time: " + (t1 - t0)/1e9);
        System.out.println("Neighbor queries: " + neighborQueryCount);
        System.out.println("Total neighbor query time (s): " + neighborQueryTimeNs / 1e9);
        System.out.println("Avg time per query (ms): " + (neighborQueryTimeNs / 1e6) / neighborQueryCount);

       // sc.close();
    }


    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId) {
        Point ghost = new Point(original.id, original.latitude, original.longitude,0);
        ghost.cellId = targetCellId;
        ghost.isLocalRegion = false; // [cite: 316] "False (boundary area)"
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
        return new Point(11,Double.parseDouble(fields[0]),
                Double.parseDouble(fields[1]),0);
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




