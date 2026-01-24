package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;

public class ExecuteDBSCAN {
    public static Result executeDBSCAN(JavaSparkContext sc, ExecutionConfiguration executionConfiguration, SparkMetricListener sparkMetricListener) {

        Result result = new Result();
        result.eps=executionConfiguration.eps;
        result.minPts=executionConfiguration.minPts;
        result.cellFactor=executionConfiguration.cellFactor;
        result.bufferFactor=executionConfiguration.bufferFactor;
        result.mergeStrategy=executionConfiguration.mergeStrategy;

        String inputPath = executionConfiguration.inputPath;

        LongAccumulator neighborQueryCount = sc.sc().longAccumulator("neighborQueryCount");
        LongAccumulator neighborQueryTimeNs = sc.sc().longAccumulator("neighborQueryTimeNs");
        LongAccumulator ghostPoints=sc.sc().longAccumulator("ghostPoints");
        long startTime = System.currentTimeMillis();

        //Reads from the file and associates each with a long index as Point(index, lat, long, clusterid =0)
        JavaRDD<Point> points = sc.textFile(inputPath)
                .zipWithIndex()
                .filter(t -> !t._1.trim().isEmpty())
                .map(t -> {
                    String[] parts = t._1.trim().split(",");
                    float x = Float.parseFloat(parts[0]);
                    float y = Float.parseFloat(parts[1]);
                    return new Point(t._2, x, y, 0);
                })
                .cache();
        points.take(20).forEach(point ->System.out.println(point));


        long totalPoints = points.count();
        System.out.println("Total points: " + totalPoints);

        String runId = String.valueOf(System.currentTimeMillis());

        // Find min/max coordinates of entire dataset
        float minLatitude = points.map(p -> p.latitude).reduce(Float::min);
        float maxLatitude = points.map(p -> p.latitude).reduce(Float::max);
        float minLongitude = points.map(p -> p.longitude).reduce(Float::min);
        float maxLongitude = points.map(p -> p.longitude).reduce(Float::max);

        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(minLatitude, maxLatitude, minLongitude, maxLongitude, result.eps);
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
            float cellMinX = minLatitude + homeX * cfg.cellSize;
            float cellMinY = minLongitude + homeY * cfg.cellSize;

            float dxLeft = p.latitude - cellMinX;
            float dxRight = (cellMinX + cfg.cellSize) - p.latitude;
            float dyBottom = p.longitude - cellMinY;
            float dyTop = (cellMinY + cfg.cellSize) - p.longitude;

            // Left Neighbor
            if (homeX > 0 && dxLeft <= cfg.buffer) {
                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX - 1),ghostPoints);
            }
            // Right Neighbor
            if (homeX < cfg.numCellsX - 1 && dxRight <= cfg.buffer) {
                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX + 1),ghostPoints);
            }
            // Bottom Neighbor
            if (homeY > 0 && dyBottom <= cfg.buffer) {
                addGhost(assignments, p, (homeY - 1) * cfg.numCellsX + homeX,ghostPoints);
            }
            // Top Neighbor
            if (homeY < cfg.numCellsY - 1 && dyTop <= cfg.buffer) {
                addGhost(assignments, p, (homeY + 1) * cfg.numCellsX + homeX,ghostPoints);
            }

            // Top-Left (homeX-1, homeY+1)
            if (homeX > 0 && homeY < cfg.numCellsY - 1 && dxLeft <= cfg.buffer && dyTop <= cfg.buffer) {
                int cellId = (homeY + 1) * cfg.numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId,ghostPoints);
            }

            // Top-Right (homeX+1, homeY+1)
            if (homeX < cfg.numCellsX - 1 && homeY < cfg.numCellsY - 1 && dxRight <= cfg.buffer && dyTop <= cfg.buffer) {
                int cellId = (homeY + 1) * cfg.numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId,ghostPoints);
            }

            // Bottom-Left (homeX-1, homeY-1)
            if (homeX > 0 && homeY > 0 && dxLeft <= cfg.buffer && dyBottom <= cfg.buffer) {
                int cellId = (homeY - 1) * cfg.numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId,ghostPoints);
            }

            // Bottom-Right (homeX+1, homeY-1)
            if (homeX < cfg.numCellsX - 1 && homeY > 0 && dxRight <= cfg.buffer && dyBottom <= cfg.buffer) {
                int cellId = (homeY - 1) * cfg.numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId,ghostPoints);
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

                            localDBSCAN(cellPoints, result.eps, result.minPts, neighborQueryCount, neighborQueryTimeNs);

                            List<Tuple2<Integer, Point>> out = new ArrayList<>();
                            for (Point p : cellPoints) {
                                out.add(new Tuple2<>(cellId, p));
                            }
                            return out.iterator();
                        })
                        .cache();
        System.out.println("Local DBSCAN Executed on each cells they belong ");
        dbscanClusteredAsPerCellsRDD.take(20).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));


        JavaPairRDD<Float, Iterable<Point>> groupedByPoint =
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

                            float dist = distance(a, b);
                            if (dist <= result.eps *result.eps ) {
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

        Map<String, Integer> localToGlobal ;
        if (result.mergeStrategy.equals("UF"))
        {
            UnionFindMerge unionFindMerge = new UnionFindMerge();
            localToGlobal=unionFindMerge.merge(samePointMergeRDD,dbscanClusteredAsPerCellsRDD);
        }else{
            GraphxMerge graphxMerge = new GraphxMerge();
            localToGlobal=graphxMerge.merge(samePointMergeRDD,dbscanClusteredAsPerCellsRDD);
        }

        Broadcast<Map<String, Integer>> bcMapToGlobalId = sc.broadcast(localToGlobal);
        JavaPairRDD<Float, Integer> pointToGlobalId =
                groupedByPoint.mapValues(pts -> {

                    Integer coreGid = null;
                    Integer borderGid = null;

                    for (Point p : pts) {
                        if (p.clusterId > 0) {
                            String key = p.cellId + "_" + p.clusterId;
                            Integer gid = bcMapToGlobalId.value().get(key);
                            if (gid == null) continue;

                            if (p.isCorePoint) coreGid = gid;
                            else borderGid = gid;
                        }
                    }
                    if (coreGid != null) return coreGid;
                    if (borderGid != null) return borderGid;
                    return -1;
                });


        JavaPairRDD<Float, Point> idPointPairRDD =
                dbscanClusteredAsPerCellsRDD
                        .mapToPair(p -> new Tuple2<>(p._2.id, p._2));

        JavaRDD<Point> finalClusters =
                idPointPairRDD
                        .join(pointToGlobalId)
                        .map(t -> {
                            Point p = t._2._1;
                            p.clusterId = t._2._2;
                            return p;
                        })
                        .filter(p -> p.isLocalRegion);
        finalClusters.coalesce(1).saveAsTextFile("output/finalClusters"+ runId);


        long endTime = System.currentTimeMillis();
        result.runtimeMs = endTime - startTime;
        result.totalPoints = finalClusters.count();
        result.ghostPoints = ghostPoints.value();
        result.neighborQueryCount = neighborQueryCount.value();
        result.neighborQueryTimeNs = neighborQueryTimeNs.value();
        result.shuffleReadBytes=sparkMetricListener.shuffleRead;
        result.shuffleWriteBytes=sparkMetricListener.shuffleWrite;
        result.diskSpilledBytes=sparkMetricListener.diskSpilled;
        result.memorySpilledBytes=sparkMetricListener.memorySpilled;
        return  result;
    }


    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId, LongAccumulator ghostPoints) {
        Point ghost = new Point(original.id, original.latitude, original.longitude, 0);
        ghost.cellId = targetCellId;
        ghost.isLocalRegion = false;
        list.add(new Tuple2<>(targetCellId, ghost));
        ghostPoints.add(1);
    }



    public static float distance(Point a, Point b) {
        float dx = a.latitude - b.latitude;
        float dy = a.longitude - b.longitude;
        return dx * dx + dy * dy;
    }


    public static void localDBSCAN(List<Point> points, float eps, int minPts,LongAccumulator queryCount, LongAccumulator queryTime) {

        Map<Float, Boolean> visited = new HashMap<>();
        int localClusterId = 0;

        for (Point p : points) {
            if (visited.getOrDefault(p.id, false)) {
                continue;
            }

            visited.put(p.id, true);
            //List<Point> neighbors = regionQuery(points, p, eps, queryCount, queryTime);
            KDTree kdTree=new KDTree(points);

            List<Point> neighbors =kdTree.radiusSearch(p,eps,queryCount,queryTime);

            if (neighbors.size() < minPts) {
                p.clusterId = -1;
                p.isCorePoint = false;
            } else {
                p.isCorePoint = true;
                localClusterId++;
                expandCluster(kdTree, p, neighbors, localClusterId, eps, minPts, visited, queryCount, queryTime);
            }
        }
    }

    public static void expandCluster(KDTree kdTree, Point p, List<Point> neighbors,
                                     int clusterId, float eps, int minPts, Map<Float, Boolean> visited,
                                     LongAccumulator queryCount, LongAccumulator queryTime) {
        p.clusterId = clusterId;

        Queue<Point> seeds = new LinkedList<>(neighbors);

        while (!seeds.isEmpty()) {
            Point q = seeds.poll();

            if (!visited.getOrDefault(q.id, false)) {
                visited.put(q.id, true);
                //List<Point> qNeighbors = regionQuery(points, q, eps, queryCount, queryTime);

                List<Point> qNeighbors =kdTree.radiusSearch(q,eps,queryCount,queryTime);
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


    public static List<Point> regionQuery(List<Point> points, Point p, float eps, LongAccumulator queryCount, LongAccumulator queryTime) {
        long start = System.nanoTime();

        List<Point> neighbors = new ArrayList<>();
        for (Point q : points) {
            if (distance(p, q) <= eps *eps ) {
                neighbors.add(q);
            }
        }

        long end = System.nanoTime();
        queryTime.add(end - start);
        queryCount.add(1);

        return neighbors;
    }

}
