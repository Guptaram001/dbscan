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
    static float eps = 0.03f;
    static int minPts = 50;
//
//    public static void main(String[] args) {
//
//        SparkConf conf = new SparkConf()
//                .setAppName("DBSCAN Analysis");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        SparkMetricListener metricListener = new SparkMetricListener();
//        sc.sc().addSparkListener(metricListener);
//        String inputPath = args.length >=1? args[0]: "src/main/resources/densired_2_shrink.csv";
//
//        LongAccumulator neighborQueryCount = sc.sc().longAccumulator("neighborQueryCount");
//        LongAccumulator neighborQueryTimeNs = sc.sc().longAccumulator("neighborQueryTimeNs");
//        LongAccumulator ghostPoints=sc.sc().longAccumulator("ghostPoints");
//        long startTime = System.currentTimeMillis();
//        float eps2=eps*eps;
//
//
//        //Reads from the file and associates each with a long index as Point(index, lat, long, clusterid =0)
//        JavaRDD<Point> points = sc.textFile(inputPath)
//                .zipWithIndex()
//                .filter(t -> !t._1.trim().isEmpty())
//                .map(t -> {
//                    String[] parts = t._1.trim().split(",");
//                    float x = Float.parseFloat(parts[0]);
//                    float y = Float.parseFloat(parts[1]);
//                    return new Point(t._2, x, y, 0);
//                })
//                .cache();
//        points.take(20).forEach(point ->System.out.println(point));
//
//
//        long totalPoints = points.count();
//        System.out.println("Total points: " + totalPoints);
//
//        String runId = String.valueOf(System.currentTimeMillis());
//
//        // Find min/max coordinates of entire dataset
//        float minLatitude = points.map(p -> p.latitude).reduce(Float::min);
//        float maxLatitude = points.map(p -> p.latitude).reduce(Float::max);
//        float minLongitude = points.map(p -> p.longitude).reduce(Float::min);
//        float maxLongitude = points.map(p -> p.longitude).reduce(Float::max);
//        float cellFactor=3;
//        float bufferFactor=1;
//
//        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(minLatitude, maxLatitude, minLongitude, maxLongitude, eps,cellFactor,   bufferFactor);
//        final Broadcast<PartitionConfiguration> broadcastPartitionConf = sc.broadcast(partitionConfiguration);
//
//        JavaPairRDD<Integer, Point> partitionedToCellsRDD = points.flatMapToPair(p -> {
//            PartitionConfiguration cfg = broadcastPartitionConf.value();
//            List<Tuple2<Integer, Point>> assignments = new ArrayList<>();
//            //Determine Home Cell (Geometric Location)
//            int homeX = (int) Math.floor((p.latitude - minLatitude) / cfg.cellSize);
//            int homeY = (int) Math.floor((p.longitude - minLongitude) / cfg.cellSize);
//            // Fix to safe bounds
//            homeX = Math.max(0, Math.min(homeX, cfg.numCellsX - 1));
//            homeY = Math.max(0, Math.min(homeY, cfg.numCellsY - 1));
//            int homeCellId = homeY * cfg.numCellsX + homeX;
//            // Add to Home Cell (Always Local)
//            Point local = new Point(p.id, p.latitude, p.longitude, 0);
//            local.cellId = homeCellId;
//            local.isLocalRegion = true;
//            assignments.add(new Tuple2<>(homeCellId, local));
//            // Check Boundaries for Neighbor Replication (Ghost Points)
//            float cellMinX = minLatitude + homeX * cfg.cellSize;
//            float cellMinY = minLongitude + homeY * cfg.cellSize;
//
//            float dxLeft = p.latitude - cellMinX;
//            float dxRight = (cellMinX + cfg.cellSize) - p.latitude;
//            float dyBottom = p.longitude - cellMinY;
//            float dyTop = (cellMinY + cfg.cellSize) - p.longitude;
//
//            // Left Neighbor
//            if (homeX > 0 && dxLeft <= cfg.buffer) {
//                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX - 1),ghostPoints);
//            }
//            // Right Neighbor
//            if (homeX < cfg.numCellsX - 1 && dxRight <= cfg.buffer) {
//                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX + 1),ghostPoints);
//            }
//            // Bottom Neighbor
//            if (homeY > 0 && dyBottom <= cfg.buffer) {
//                addGhost(assignments, p, (homeY - 1) * cfg.numCellsX + homeX,ghostPoints);
//            }
//            // Top Neighbor
//            if (homeY < cfg.numCellsY - 1 && dyTop <= cfg.buffer) {
//                addGhost(assignments, p, (homeY + 1) * cfg.numCellsX + homeX,ghostPoints);
//            }
//
//            // Top-Left (homeX-1, homeY+1)
//            if (homeX > 0 && homeY < cfg.numCellsY - 1 && dxLeft <= cfg.buffer && dyTop <= cfg.buffer) {
//                int cellId = (homeY + 1) * cfg.numCellsX + (homeX - 1);
//                addGhost(assignments, p, cellId,ghostPoints);
//            }
//
//            // Top-Right (homeX+1, homeY+1)
//            if (homeX < cfg.numCellsX - 1 && homeY < cfg.numCellsY - 1 && dxRight <= cfg.buffer && dyTop <= cfg.buffer) {
//                int cellId = (homeY + 1) * cfg.numCellsX + (homeX + 1);
//                addGhost(assignments, p, cellId,ghostPoints);
//            }
//
//            // Bottom-Left (homeX-1, homeY-1)
//            if (homeX > 0 && homeY > 0 && dxLeft <= cfg.buffer && dyBottom <= cfg.buffer) {
//                int cellId = (homeY - 1) * cfg.numCellsX + (homeX - 1);
//                addGhost(assignments, p, cellId,ghostPoints);
//            }
//
//            // Bottom-Right (homeX+1, homeY-1)
//            if (homeX < cfg.numCellsX - 1 && homeY > 0 && dxRight <= cfg.buffer && dyBottom <= cfg.buffer) {
//                int cellId = (homeY - 1) * cfg.numCellsX + (homeX + 1);
//                addGhost(assignments, p, cellId,ghostPoints);
//            }
//            return assignments.iterator();
//        });
//
//        System.out.println("Points Partitioned based on home cell ");
//        partitionedToCellsRDD.take(20).forEach(pair -> System.out.println(pair._1()+" "+pair._2()));
//
//        //Groups each points based on the cells they belong to and execute DBSCAN locally.
//        JavaPairRDD<Integer, Point> dbscanClusteredAsPerCellsRDD =
//                partitionedToCellsRDD
//                        .groupByKey()
//                        .flatMapToPair(cell -> {
//
//                            int cellId = cell._1;
//                            List<Point> cellPoints = new ArrayList<>();
//                            cell._2.forEach(cellPoints::add);
//
//                            Utils.localDBSCAN(cellPoints, eps2, minPts, neighborQueryCount, neighborQueryTimeNs);
//
//                            List<Tuple2<Integer, Point>> out = new ArrayList<>();
//                            for (Point p : cellPoints) {
//                                out.add(new Tuple2<>(cellId, p));
//                            }
//                            return out.iterator();
//                        })
//                        .cache();
//        System.out.println("Local DBSCAN Executed on each cells they belong ");
//        dbscanClusteredAsPerCellsRDD.take(20).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));
//
//
//        JavaPairRDD<Long, Iterable<Point>> groupedByPoint =
//                dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2))
//                        .groupByKey();
//        System.out.println("Grouped by points by after local DBSCAN executed on each cells to initiate merge ie Same point in multiple cells");
//        groupedByPoint.take(200).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));
//
//
//        JavaPairRDD<String, String> samePointMergeRDD =
//                groupedByPoint.flatMapToPair(entry -> {
//
//                    List<Point> pts = new ArrayList<>();
//                    entry._2.forEach(pts::add);
//
//                    List<Tuple2<String, String>> merges = new ArrayList<>();
//
//                    for (int i = 0; i < pts.size(); i++) {
//                        for (int j = i + 1; j < pts.size(); j++) {
//                            Point a = pts.get(i);
//                            Point b = pts.get(j);
//
//                            if (a.clusterId <= 0 || b.clusterId <= 0)
//                                continue;
//                            if (!(a.isCorePoint || b.isCorePoint))
//                                continue;
//
//                            float dist = Utils.distance(a, b);
//                            if (dist <= eps2) {
//                                String keyA = a.cellId + "_" + a.clusterId;
//                                String keyB = b.cellId + "_" + b.clusterId;
//                                if (!keyA.equals(keyB))
//                                    merges.add(new Tuple2<>(keyA, keyB));
//                            }
//                        }
//                    }
//                    return merges.iterator();
//                });
//
//        System.out.println("Merging points that are boundary and core points in different cells");
//        samePointMergeRDD.take(200).forEach(pair -> System.out.println("Edges: "+pair._1() + ": " + pair._2()));
//
//
//        List<Tuple2<String, String>> samePointMergeRDDList = samePointMergeRDD.collect();
//        JavaPairRDD<Integer, Point> boundaryOnlyRDD = dbscanClusteredAsPerCellsRDD.filter(t -> !t._2.isLocalRegion);
//        JavaPairRDD<Integer, Iterable<Point>> boundaryPointsByCellRDD = boundaryOnlyRDD.groupByKey();
//        //Merge core to non core or core with different points in a cell.
////        List<Tuple2<String, String>> differentPointsMergeRDD =
////                boundaryPointsByCellRDD
////                        .flatMap(cellGroup -> {
////                            List<Point> pts = new ArrayList<>();
////                            cellGroup._2.forEach(pts::add);
////
////                            List<Tuple2<String, String>> merges = new ArrayList<>();
////
////                            for (int i = 0; i < pts.size(); i++) {
////                                for (int j = i + 1; j < pts.size(); j++) {
////
////                                    Point a = pts.get(i);
////                                    Point b = pts.get(j);
////
////                                    if (a.clusterId <= 0 || b.clusterId <= 0)
////                                        continue;
////
////                                    if (!(a.isCorePoint || b.isCorePoint))
////                                        continue;
////
////                                    float dist = distance(a, b);
////
////                                    if (dist <= eps*eps) {
////                                        String keyA = a.cellId + "_" + a.clusterId;
////                                        String keyB = b.cellId + "_" + b.clusterId;
////
////                                        if (!keyA.equals(keyB)) {
////                                            merges.add(new Tuple2<>(keyA, keyB));
////                                        }
////                                    }
////                                }
////                            }
////
////                            return merges.iterator();
////                        })
////                        .distinct()
////                        .collect();
//
//        UnionFindString uff = new UnionFindString();
//        for (Tuple2<String, String> e : samePointMergeRDDList) {
//            uff.union(e._1, e._2);
//        }
//        System.out.println("Union Find String: After Insertion" );
//        System.out.println(uff.parent);
//        System.out.println("Total KeySet: "+uff.parent.keySet().size()+" And Values:  "+uff.parent.keySet());
//
////        for (Tuple2<String, String> e : differentPointsMergeRDD) {
////            uff.union(e._1, e._2);
////        }
//
//        Map<String, String> keyToRoot = new HashMap<>();
//        for (String k : uff.parent.keySet()) {
//            keyToRoot.put(k, uff.find(k));
//        }
//        System.out.println("Union Find String: After keyToRoot Mapping" );
//        System.out.println(uff.parent);
//
//
//        //Add isolated clusters
//        for (Point p : dbscanClusteredAsPerCellsRDD.values().distinct().collect()) {
//            if (p.clusterId > 0) {
//                String k = p.cellId + "_" + p.clusterId;
//                keyToRoot.putIfAbsent(k, k);
//            }
//        }
//
//        System.out.println("keytoroot full"+keyToRoot.size());
//        for (String k : keyToRoot.keySet()) {
//            System.out.println(k + " : " + keyToRoot.get(k));
//        }
//
//        Map<String, Integer> rootToGlobal = new HashMap<>();
//        int nnext = 1;
//        for (String rep : new HashSet<>(keyToRoot.values())) {
//            rootToGlobal.put(rep, ++nnext);
//        }
//        System.out.println("rootToGlobal "+rootToGlobal.size());
//        for (String k : rootToGlobal.keySet()) {
//            System.out.println(k + " : " + rootToGlobal.get(k));
//        }
//
//        Map<String, Integer> edgesToGlobal = new HashMap<>();
//        for (Map.Entry<String, String> e : keyToRoot.entrySet()) {
//            edgesToGlobal.put(e.getKey(), rootToGlobal.get(e.getValue()));
//        }
//
//        Broadcast<Map<String, Integer>> bcMapToGlobalId = sc.broadcast(edgesToGlobal);
//
//        JavaPairRDD<Long, Integer> pointToGlobalId =
//                groupedByPoint.mapValues(pts -> {
//
//                    boolean hasCore = false;
//                    boolean hasBorder = false;
//                    Integer coreGid = null;
//                    Integer borderGid = null;
//
//                    for (Point p : pts) {
//                        if (p.clusterId > 0) {
//                            // Map local cluster -> representative -> global id
//                            String key = p.cellId + "_" + p.clusterId;
//                            Integer gid = bcMapToGlobalId.value().get(key);
//                            if (gid == null) continue;
//
//                            if (p.isCorePoint) {
//                                hasCore = true;
//                                coreGid = gid;
//                            } else {
//                                hasBorder = true;
//                                borderGid = gid;
//                            }
//                        }
//                    }
//
//                    if (hasCore) {
//                        return coreGid;
//                    }
//
//                    if (hasBorder) {
//                        return borderGid;
//                    }
//
//                    return -1;
//                });
//        System.out.println("pointToGlobalId result"+pointToGlobalId.collect().size());
//        pointToGlobalId.take(50).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));
//
//        JavaPairRDD<Long, Point> idPointPairRDD = dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2));
//        System.out.println("idPointPairRDD result"+idPointPairRDD.collect().size());
//        idPointPairRDD.take(50).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));
//
//
//        JavaRDD<Point> finalClusters = idPointPairRDD.join(pointToGlobalId)
//                .map(t -> {
//                    Point p = t._2._1;
//                    p.clusterId = t._2._2;
//                    return p;
//                })
//                .filter(p -> p.isLocalRegion);
//
//        long endTime = System.currentTimeMillis();
//
//        System.out.println("Time taken: " + (endTime - startTime) + " ms");
//        System.out.println("FinalClusters count = " + finalClusters.count());
//        long ghostCountRDD = boundaryOnlyRDD.count();
//        System.out.println("Ghost points via RDD count = " + ghostCountRDD);
//        //System.out.println("Ghost points via accumulator = " + ghostPoints.value());
//        System.out.println("Query count = " + neighborQueryCount);
//        System.out.println("QueryTime = " + neighborQueryTimeNs);
//        System.out.println(metricListener);
//
//
//        finalClusters.coalesce(1).saveAsTextFile("output/finalClusterss"+ runId);
//        long localCount = finalClusters.filter(p -> p.isLocalRegion).count();
//        System.out.println("Local: " + localCount);
//
//        long boundaryCount = dbscanClusteredAsPerCellsRDD.filter(p-> !p._2.isLocalRegion).count();
//        System.out.println("Boundary: " + boundaryCount);
//        long t0 = System.nanoTime();
//        List<Point> result = dbscanClusteredAsPerCellsRDD.values().collect();
//        long t1 = System.nanoTime();
//        System.out.println("Collected size: " + result.size());
//        System.out.println("DBSCAN time: " + (t1 - t0) / 1e9);
//        System.out.println("Neighbor queries: " + neighborQueryCount);
//
//        sc.close();
//    }
//
//    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId,LongAccumulator ghostPoints) {
//        Point ghost = new Point(original.id, original.latitude, original.longitude, 0);
//        ghost.cellId = targetCellId;
//        ghost.isLocalRegion = false;
//        list.add(new Tuple2<>(targetCellId, ghost));
//        ghostPoints.add(1);
//    }
}




