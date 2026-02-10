package spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Graph;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;


public class TestGraphx {
    static float eps = 0.03f;
    static int minPts = 50;
    static final boolean DEBUG = true;
//
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf()
//                .setAppName("DBSCAN Analysis");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        String inputPath = args.length >=1? args[0]: "src/main/resources/densired_2.csv";
//        float eps2=eps*eps;
//        //JavaRDD<String> rawLines = sc.textFile(args[0]);
//
////        SparkConf conf = new SparkConf().setAppName("DBSCAN Analysis");
////        String inputPath = args.length > 0 ? args[0] : "densired_2_shrink.csv";
////        JavaRDD<String> rawLines = sc.textFile(inputPath);
////        SparkConf conf = new SparkConf().setAppName("DBSCAN Analysis").setMaster("local[*]").set("spark.driver.maxResultSize", "4g");
////        JavaSparkContext sc = new JavaSparkContext(conf);
////        JavaRDD<String> rawLines = sc.textFile("src/main/resources/densired_2_shrink.csv");
////        JavaRDD<String> rawLines = sc.textFile("src/main/resources/test.txt");
//
//        LongAccumulator neighborQueryCount = sc.sc().longAccumulator("neighborQueryCount");
//        LongAccumulator neighborQueryTimeNs = sc.sc().longAccumulator("neighborQueryTimeNs");
//        LongAccumulator ghostPoints = sc.sc().longAccumulator("ghostPoints");
//        long startTime = System.currentTimeMillis();
//
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
//        if (DEBUG)
//            points.take(20).forEach(point ->System.out.println(point));
//
//
//        long totalPoints = points.count();
//        if (DEBUG)
//            System.out.println("Total points: " + totalPoints);
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
//        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(minLatitude, maxLatitude, minLongitude, maxLongitude, eps,cellFactor,bufferFactor);
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
//        if (DEBUG) {
//            System.out.println("Points Partitioned based on home cell ");
//            partitionedToCellsRDD.take(20).forEach(pair -> System.out.println(pair._1() + " " + pair._2()));
//        }
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
//
//                            List<Tuple2<Integer, Point>> out = new ArrayList<>();
//                            for (Point p : cellPoints) {
//                                out.add(new Tuple2<>(cellId, p));
//                            }
//                            return out.iterator();
//                        })
//                        .cache();
//        if (DEBUG)
//        {
//            System.out.println("Local DBSCAN Executed on each cells they belong ");
//            dbscanClusteredAsPerCellsRDD.take(20).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));
//
//        }
//
//
//        JavaPairRDD<Long, Iterable<Point>> groupedByPoint =
//                dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2))
//                        .groupByKey();
//        if (DEBUG)
//        {
//            System.out.println("Grouped by points by after local DBSCAN executed on each cells to initiate merge ie Same point in multiple cells");
//           groupedByPoint.take(20).forEach(pair ->  System.out.println(pair._1() + ": " + pair._2()));
//
//        }
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
////        samePointMergeRDD:{(12_1, 13_2),(13_2, 14_1),(15_3, 16_1)}
//        if (DEBUG)
//            samePointMergeRDD.coalesce(1).saveAsTextFile("output/samePointMergeRDD" + runId);
//
//
//        JavaPairRDD<Integer, Point> boundaryOnlyRDD = dbscanClusteredAsPerCellsRDD.filter(t -> !t._2.isLocalRegion);
//
//        // 1) All local clusters (vertices must include isolated ones too)
//        JavaRDD<String> allLocalClusters = dbscanClusteredAsPerCellsRDD.values()
//                .filter(p -> p.clusterId > 0)
//                .map(p -> p.cellId + "_" + p.clusterId)
//                .distinct()
//                .cache();
////        allLocalClusters:{12_1 ,13_2 ,14_1 ,15_3 ,16_1}
//        if (DEBUG) {
//            allLocalClusters.coalesce(1).saveAsTextFile("output/allLocalClusters_" + runId);
//            System.out.println("Total local clusters: " + allLocalClusters.count());
//        }
//// 2) Assign a vertexId to every local cluster label
//        JavaPairRDD<String, Long> labelToVid = allLocalClusters.zipWithIndex()
//                .mapToPair(t -> new Tuple2<>(t._1, t._2))
//                .cache();
//        if (DEBUG) System.out.println("labelToVid: " + labelToVid.count());
////        labelToVid:{12_1->0,13_2->1,14_1->2,15_3->3,16_1->4}
//        if (DEBUG) labelToVid.coalesce(1).saveAsTextFile("output/labelToVid" + runId);
//
//
//// vertices: (vid, attr). attr can be anything
//        JavaRDD<scala.Tuple2<Object, Long>> vertices = labelToVid
//                .map(t -> new scala.Tuple2<Object, Long>(t._2, t._2));
//        if (DEBUG) System.out.println("vertices: " + vertices.count());
//        vertices.take(50).forEach(pair -> {System.out.println(pair._1() + ": " + pair._2());});
////        Vertices:{0->0,1->1,2->2,3->3,4->4}
//        if (DEBUG) vertices.coalesce(1).saveAsTextFile("output/vertices" + runId);
//
//
//// 3) Build edges WITHOUT collect/broadcast, using joins
//
//        JavaPairRDD<String, String> undirectedEdges = samePointMergeRDD.flatMapToPair(e ->
//                Arrays.asList(
//                        new Tuple2<>(e._1, e._2),
//                        new Tuple2<>(e._2, e._1)
//                ).iterator()
//        ).distinct();
////        undirectedEdges:{(12_1, 13_2),(13_2, 12_1),(13_2, 14_1),(14_1, 13_2)(15_3, 16_1),(16_1, 15_3)}
//        if (DEBUG) undirectedEdges.coalesce(1).saveAsTextFile("output/undirectedEdges" + runId);
//
//
//        if (DEBUG) System.out.println("undirectedEdges: " + undirectedEdges.count());
//        undirectedEdges.take(50).forEach(pair -> {System.out.println(pair._1() + ": " + pair._2());});
//
//// Map src label -> src vid
//        JavaPairRDD<String, Tuple2<String, Long>> srcWithVid = undirectedEdges
//                .join(labelToVid);  // (srcLabel, (dstLabel, srcVid))
////        undirectedEdges:{(12_1, 13_2),(13_2, 12_1),(13_2, 14_1),(14_1, 13_2)(15_3, 16_1),(16_1, 15_3)}
////        labelToVid:{12_1->0,13_2->1,14_1->2,15_3->3,16_1->4}
////        srcWithVid:{(12_1, (13_2,0)),(13_2, (12_1,1)),(13_2, (14_1,1),(14_1, (13_2,2)),(15_3, (16_1,3)),(16_1, (15_3,4))}
//
//
//        if (DEBUG) System.out.println("srcWithVid: " + srcWithVid.count());
//        if (DEBUG) srcWithVid.take(50).forEach(pair -> {System.out.println(pair._1() + ": " + pair._2()+" "+pair._2._1 + ": " + pair._2._2);});
//        if (DEBUG) srcWithVid.coalesce(1).saveAsTextFile("output/srcWithVid" + runId);
//
//// Key by dstLabel to join dst vid
//        JavaPairRDD<String, Tuple2<String, Long>> keyedByDst = srcWithVid
//                .mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2)));
//        if (DEBUG) System.out.println("keyedByDst: " + keyedByDst.count());
//        //keyedByDst.take(50).forEach(pair -> {System.out.println(pair._1() + ": " + pair._2()+" "+pair._2._1 + ": " + pair._2._2);});
////        srcWithVid:{(12_1, (13_2,0)),(13_2, (12_1,1)),(13_2, (14_1,1),(14_1, (13_2,2)),(15_3, (16_1,3)),(16_1, (15_3,4))}
////        keyedByDst:{(13_2, (12_1,0)),(12_1, (13_2,1)),(14_1, (13_2,1),(13_2, (14_1,2)),(16_1, (15_3,3)),(15_3, (16_1,4))}
//
//// (dstLabel, (srcLabel, srcVid))
//        if (DEBUG) keyedByDst.coalesce(1).saveAsTextFile("output/keyedByDst"  + runId);
//
//        JavaRDD<Edge<Long>> edges = keyedByDst
//                .join(labelToVid) // (dstLabel, ((srcLabel, srcVid), dstVid))
//                .map(t -> new Edge<>(t._2._1._2, t._2._2, 1L))
//                .distinct();
//
//        if (DEBUG) edges.coalesce(1).saveAsTextFile("output/edges" + runId);
//        if (DEBUG) System.out.println("Graph edges: " + edges.count());
//        if (DEBUG) System.out.println("edges: " + edges.count());
//        //edges.take(50).forEach(pair -> {System.out.println(pair);});
//
//
//// 4) Build Graph and run Connected Components
//        ClassTag<Long> longTag = ClassTag$.MODULE$.apply(Long.class);
//
//        Graph<Long, Long> graph = Graph.apply(
//                vertices.rdd(),
//                edges.rdd(),
//                0L,
//                StorageLevel.MEMORY_AND_DISK(),
//                StorageLevel.MEMORY_AND_DISK(),
//                longTag,
//                longTag
//        );
//
//
//        Graph<Object, Long> cc = ConnectedComponents.run(graph, longTag, longTag);
//
//// vertexToComponent: (vid -> componentVid)
//        JavaPairRDD<Long, Long> vidToComp = cc.vertices().toJavaRDD()
//                .mapToPair(t -> new Tuple2<>((Long) t._1, (Long) t._2))
//                .cache();
//        if (DEBUG) System.out.println("vidToComp: " + vidToComp.count());
//        if (DEBUG) vidToComp.coalesce(1).saveAsTextFile("output/vidToComp" + runId);
//        //vidToComp.take(50).forEach(pair -> {System.out.println(pair._1+" "+ pair._2);});
//
//// 5) Convert back: (label -> componentVid)
//        JavaPairRDD<Long, String> vidToLabel = labelToVid.mapToPair(t -> new Tuple2<>(t._2, t._1));
//        if (DEBUG) System.out.println("vidToLabel: " + vidToLabel.count());
//        if (DEBUG) vidToLabel.coalesce(1).saveAsTextFile("output/vidToLabel" + runId);
//        //vidToLabel.take(50).forEach(pair -> {System.out.println(pair._1+" "+ pair._2);});
//
//
//        JavaPairRDD<String, Long> labelToComp = vidToLabel
//                .join(vidToComp)          // (vid, (label, comp))
//                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
//                .cache();
//        if (DEBUG) System.out.println("labelToComp: " + labelToComp.count());
//        if (DEBUG) labelToComp.coalesce(1).saveAsTextFile("output/labelToComp" + runId);
//        //labelToComp.take(50).forEach(pair -> {System.out.println(pair._1+" "+ pair._2);});
//
//
//        if (DEBUG) System.out.println("Distinct components: " + labelToComp.values().distinct().count());
//
//
//// 6) Assign sequential global IDs to each component
//        JavaPairRDD<Long, Integer> compToGlobal = labelToComp.values()
//                .distinct()
//                .zipWithIndex()
//                .mapToPair(t -> new Tuple2<>(t._1, (int) (t._2 + 1)));
//        if (DEBUG) System.out.println("compToGlobal: " + compToGlobal.count());
//        if (DEBUG) compToGlobal.coalesce(1).saveAsTextFile("output/compToGlobal" + runId);
//        //compToGlobal.take(50).forEach(pair -> {System.out.println(pair._1+" "+ pair._2);});
//
//
//        JavaPairRDD<String, Integer> localToGlobal = labelToComp
//                .mapToPair(t -> new Tuple2<>(t._2, t._1))   // (comp, label)
//                .join(compToGlobal)                          // (comp, (label, gid))
//                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
//                .cache();
//        if (DEBUG) System.out.println("localToGlobal: " + localToGlobal.count());
//        if (DEBUG) localToGlobal.coalesce(1).saveAsTextFile("output/localToGlobal" + runId);
//       // localToGlobal.take(50).forEach(pair -> {System.out.println(pair._1+" "+ pair._2);});
//
//
//        // Assign global cluster IDs to points
//        JavaPairRDD<Long, String> pointToLocalKey = groupedByPoint.mapValues(pts -> {
//            String coreKey = null;
//            String borderKey = null;
//
//            for (Point p : pts) {
//                if (p.clusterId > 0) {
//                    String k = p.cellId + "_" + p.clusterId;
//                    if (p.isCorePoint) coreKey = k;
//                    else borderKey = k;
//                }
//            }
//            return coreKey != null ? coreKey : borderKey;
//        });
//        try {
//            if (DEBUG) System.out.println("pointolocal "+pointToLocalKey.count());
//           // if (DEBUG) pointToLocalKey.take(50).forEach(pair -> System.out.println("pointfolocal Map: "+pair._1() + ": " + pair._2()));
//        } catch (Exception e) {
//            System.err.println("ERROR in pointToLocalKey: " + e.getMessage());
//            e.printStackTrace();
//            System.exit(1);
//        }
//
//
//        JavaPairRDD<Long, Integer> pointToGlobal = pointToLocalKey
//                .filter(t -> t._2 != null)
//                .mapToPair(t -> new Tuple2<>(t._2, t._1))
//                .join(localToGlobal)
//                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2));
//        if (DEBUG) System.out.println("pointoglobal count"+pointToGlobal.values().distinct().count());
//        //if (DEBUG) pointToGlobal.take(50).forEach(pair ->  System.out.println("pointtoglobal Map: "+pair._1() + ": " + pair._2()));
//
//
//        JavaPairRDD<Long, Integer> pointToGlobalUnique =
//                pointToGlobal
//                        .reduceByKey((a, b) -> a); // same globalId anyway
//
//        // Final output
//        JavaPairRDD<Long, Point> localOnly = dbscanClusteredAsPerCellsRDD
//                .filter(t -> t._2.isLocalRegion)
//                .mapToPair(t -> new Tuple2<>(t._2.id, t._2));
//
//        JavaRDD<Point> finalClusters = localOnly
//                .leftOuterJoin(pointToGlobalUnique)
//                .map(t -> {
//                    Point p = t._2._1;
//                    p.clusterId = t._2._2.orElse(-1);
//                    return p;
//                });
//
//        long endTime = System.currentTimeMillis();
//        if (DEBUG) System.out.println("Time taken: " + (endTime - startTime) + " ms");
//        long finalCount = finalClusters.count();
//        if (DEBUG) System.out.println("Final clusters count: " + finalCount);
//        long ghostCountRDD = boundaryOnlyRDD.count();
//        if (DEBUG) System.out.println("Ghost points via RDD count = " + ghostCountRDD);
//        if (DEBUG) System.out.println("Query count = " + neighborQueryCount);
//        if (DEBUG) System.out.println("QueryTime = " + neighborQueryTimeNs);
//        finalClusters.coalesce(1).saveAsTextFile("output/finalClusters_" + runId);
//        if (DEBUG) System.out.println("localToGlobal size: " + localToGlobal.count());
//        if (DEBUG) System.out.println("Global clusters: " + localToGlobal.values().distinct().count());
//        sc.close();
//    }
//
//
//    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId,LongAccumulator ghostPoints) {
//        Point ghost = new Point(original.id, original.latitude, original.longitude, 0);
//        ghost.cellId = targetCellId;
//        ghost.isLocalRegion = false;
//        list.add(new Tuple2<>(targetCellId, ghost));
//        ghostPoints.add(1);
//    }
}




