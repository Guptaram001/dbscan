package spark;
import com.twitter.chill.KryoSerializer;
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
    static double eps = 0.03;
    static int minPts = 50;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("DBSCAN Analysis")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.kryo.registrator", "MyRegistrator");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = args.length >=1? args[0]: "src/main/resources/densired_2.csv";
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

        // 1) All local clusters (vertices must include isolated ones too)
        JavaRDD<String> allLocalClusters = dbscanClusteredAsPerCellsRDD.values()
                .filter(p -> p.clusterId > 0)
                .map(p -> p.cellId + "_" + p.clusterId)
                .distinct()
                .cache();

        System.out.println("Total local clusters (vertices): " + allLocalClusters.count());

// 2) Assign a vertexId to every local cluster label
        JavaPairRDD<String, Long> labelToVid = allLocalClusters.zipWithIndex()
                .mapToPair(t -> new Tuple2<>(t._1, t._2))
                .cache();

// vertices: (vid, attr). attr can be anything (we keep vid)
        JavaRDD<scala.Tuple2<Object, Long>> vertices = labelToVid
                .map(t -> new scala.Tuple2<Object, Long>(t._2, t._2));

// 3) Build edges WITHOUT collect/broadcast, using joins
// samePointMergeRDD: (A, B)

        JavaPairRDD<String, String> undirectedEdges = samePointMergeRDD.flatMapToPair(e ->
                Arrays.asList(
                        new Tuple2<>(e._1, e._2),
                        new Tuple2<>(e._2, e._1)
                ).iterator()
        ).distinct();

// Map src label -> src vid
        JavaPairRDD<String, Tuple2<String, Long>> srcWithVid = undirectedEdges
                .join(labelToVid);  // (srcLabel, (dstLabel, srcVid))

// Key by dstLabel to join dst vid
        JavaPairRDD<String, Tuple2<String, Long>> keyedByDst = srcWithVid
                .mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2)));
// (dstLabel, (srcLabel, srcVid))

        JavaRDD<Edge<Long>> edges = keyedByDst
                .join(labelToVid) // (dstLabel, ((srcLabel, srcVid), dstVid))
                .map(t -> new Edge<>(t._2._1._2, t._2._2, 1L))
                .distinct();

        System.out.println("Graph edges: " + edges.count());

// 4) Build Graph and run Connected Components
        ClassTag<Long> longTag = ClassTag$.MODULE$.apply(Long.class);

        Graph<Long, Long> graph = Graph.apply(
                vertices.rdd(),
                edges.rdd(),
                0L,
                StorageLevel.MEMORY_AND_DISK(),
                StorageLevel.MEMORY_AND_DISK(),
                longTag,
                longTag
        );

        Graph<Object, Long> cc = ConnectedComponents.run(graph, longTag, longTag);

// vertexToComponent: (vid -> componentVid)
        JavaPairRDD<Long, Long> vidToComp = cc.vertices().toJavaRDD()
                .mapToPair(t -> new Tuple2<>((Long) t._1, (Long) t._2))
                .cache();

// 5) Convert back: (label -> componentVid)
        JavaPairRDD<Long, String> vidToLabel = labelToVid.mapToPair(t -> new Tuple2<>(t._2, t._1));

        JavaPairRDD<String, Long> labelToComp = vidToLabel
                .join(vidToComp)          // (vid, (label, comp))
                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
                .cache();

        System.out.println("Distinct components: " + labelToComp.values().distinct().count());


// 6) Assign sequential global IDs to each component
        JavaPairRDD<Long, Integer> compToGlobal = labelToComp.values()
                .distinct()
                .zipWithIndex()
                .mapToPair(t -> new Tuple2<>(t._1, (int) (t._2 + 1)));

        JavaPairRDD<String, Integer> localToGlobal = labelToComp
                .mapToPair(t -> new Tuple2<>(t._2, t._1))   // (comp, label)
                .join(compToGlobal)                          // (comp, (label, gid))
                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
                .cache();

        // Assign global cluster IDs to points
        JavaPairRDD<Double, String> pointToLocalKey = groupedByPoint.mapValues(pts -> {
            String coreKey = null;
            String borderKey = null;

            for (Point p : pts) {
                if (p.clusterId > 0) {
                    String k = p.cellId + "_" + p.clusterId;
                    if (p.isCorePoint) coreKey = k;
                    else borderKey = k;
                }
            }
            return coreKey != null ? coreKey : borderKey;
        });
        try {
            System.out.println("pointolocal "+pointToLocalKey.count());
            pointToLocalKey.take(2000).forEach(pair -> System.out.println("pointfolocal Map: "+pair._1() + ": " + pair._2()));
        } catch (Exception e) {
            System.err.println("ERROR in pointToLocalKey: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }


        JavaPairRDD<Double, Integer> pointToGlobal = pointToLocalKey
                .filter(t -> t._2 != null)
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .join(localToGlobal)
                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2));
        System.out.println("pointoglobal count"+pointToGlobal.values().distinct().count());
        pointToGlobal.take(2000).forEach(pair -> System.out.println("pointtoglobal Map: "+pair._1() + ": " + pair._2()));


        JavaPairRDD<Double, Integer> pointToGlobalUnique =
                pointToGlobal
                        .reduceByKey((a, b) -> a); // same globalId anyway

        // Final output
        JavaPairRDD<Double, Point> localOnly = dbscanClusteredAsPerCellsRDD
                .filter(t -> t._2.isLocalRegion)
                .mapToPair(t -> new Tuple2<>(t._2.id, t._2));

        JavaRDD<Point> finalClusters = localOnly
                .leftOuterJoin(pointToGlobalUnique)
                .map(t -> {
                    Point p = t._2._1;
                    p.clusterId = t._2._2.orElse(-1);
                    return p;
                });

        System.out.println("About to count finalClusters...");
        long finalCount = finalClusters.count();
        System.out.println("Final clusters count: " + finalCount);
        finalClusters.coalesce(1).saveAsTextFile("output/finalClusters_" + runId);
        System.out.println("localToGlobal size: " + localToGlobal.count());
        System.out.println("Global clusters: " + localToGlobal.values().distinct().count());
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

}




