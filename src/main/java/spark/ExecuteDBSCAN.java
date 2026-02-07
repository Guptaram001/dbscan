package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;

public class ExecuteDBSCAN {
    //Parallel DBSCAN
    public static Result executeDBSCAN(JavaSparkContext sc, ExecutionConfiguration executionConfiguration, SparkMetricListener sparkMetricListener) {

         final float EPS = 1e-6f;

        Result result = new Result();
        result.eps=executionConfiguration.eps;
        result.minPts=executionConfiguration.minPts;
        result.cellFactor=executionConfiguration.cellFactor;
        result.bufferFactor=executionConfiguration.bufferFactor;
        result.mergeStrategy=executionConfiguration.mergeStrategy;
        float eps2=result.eps*result.eps;
        float eps=executionConfiguration.eps;
        boolean DEBUG=executionConfiguration.DEBUG;

        String inputPath = executionConfiguration.inputPath;

        LongAccumulator neighborQueryCount = sc.sc().longAccumulator("neighborQueryCount");
        LongAccumulator neighborQueryTimeNs = sc.sc().longAccumulator("neighborQueryTimeNs");
        LongAccumulator ghostPoints=sc.sc().longAccumulator("ghostPoints");
        long startTime = System.currentTimeMillis();

        //Reads from the file and associates each with a long index as Point(index, lat, long, clusterid =0)
        long readStart = System.currentTimeMillis();
        JavaRDD<Point> points=readPoints(sc,inputPath);
        points.count();
        long readEnd = System.currentTimeMillis();


        long totalPoints = points.count();
        if (DEBUG)
            System.out.println("Total points: " + totalPoints);

        String runId = String.valueOf(System.currentTimeMillis());

        // Find min/max coordinates of entire dataset
        float minLatitude = points.map(p -> p.latitude).reduce(Float::min);
        float maxLatitude = points.map(p -> p.latitude).reduce(Float::max);
        float minLongitude = points.map(p -> p.longitude).reduce(Float::min);
        float maxLongitude = points.map(p -> p.longitude).reduce(Float::max);

        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(minLatitude, maxLatitude, minLongitude, maxLongitude,result.eps, executionConfiguration.cellFactor, executionConfiguration.bufferFactor);
        final Broadcast<PartitionConfiguration> broadcastPartitionConf = sc.broadcast(partitionConfiguration);

        JavaPairRDD<Integer, Point> partitionedToCellsRDD=partitionPointsToCells(points, minLatitude, minLongitude, broadcastPartitionConf, ghostPoints, EPS);
        if (DEBUG){
            System.out.println("Points Partitioned based on home cell ");
            partitionedToCellsRDD.take(20).forEach(pair -> System.out.println(pair._1()+" "+pair._2()));
            partitionedToCellsRDD.coalesce(1).saveAsTextFile("output/partitionedToCellsRDD"+runId);
        }

        //Groups each points based on the cells they belong to and execute DBSCAN locally.
        JavaPairRDD<Integer, Point> dbscanClusteredAsPerCellsRDD = dbscanClusteredAsPerCells(partitionedToCellsRDD,eps2,result.minPts,neighborQueryCount,neighborQueryTimeNs);
        if (DEBUG){
            System.out.println("Local DBSCAN Executed on each cells they belong ");
            dbscanClusteredAsPerCellsRDD.take(20).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));
            dbscanClusteredAsPerCellsRDD.distinct().coalesce(1).saveAsTextFile("output/dbscan"+ runId);
        }


        JavaPairRDD<Long, Iterable<Point>> groupedByPoint =
                dbscanClusteredAsPerCellsRDD.mapToPair(p -> new Tuple2<>(p._2.id, p._2))
                        .groupByKey();
        if (DEBUG){
            System.out.println("Grouped by points by after local DBSCAN executed on each cells to initiate merge ie Same point in multiple cells");
            groupedByPoint.take(200).forEach(pair -> System.out.println(pair._1() + ": " + pair._2()));
            groupedByPoint.distinct().coalesce(1).saveAsTextFile("output/groupedBy"+ runId);
        }


        JavaPairRDD<String, String> samePointMergeRDD= samePointMerge(groupedByPoint);
        if (DEBUG){
            System.out.println("Merging points that are boundary and core points in different cells");
            samePointMergeRDD.take(200).forEach(pair -> System.out.println("Edges: "+pair._1() + ": " + pair._2()));
            samePointMergeRDD.distinct().coalesce(1).saveAsTextFile("output/samePointMergeRDD"+ runId);
        }

//        JavaPairRDD<Integer, Point> boundaryPointsRDD =
//                dbscanClusteredAsPerCellsRDD
//                        .filter(t -> !t._2.isLocalRegion && t._2.clusterId > 0)  // Only boundary points with clusters
//                        .mapToPair(t -> new Tuple2<>(t._1, t._2));  // (cellId, Point)
//
//        JavaPairRDD<String, String> crossCellMergeRDD =
//                boundaryPointsRDD
//                        .flatMapToPair(cellPoint -> {
//                            int cellId = cellPoint._1;
//                            Point p = cellPoint._2;
//
//                            List<Tuple2<String, Point>> resultt = new ArrayList<>();
//
//                            int gridX = (int) Math.floor((p.latitude - minLatitude) / eps);
//                            int gridY = (int) Math.floor((p.longitude - minLongitude) / eps);
//
//                            // Emit to neighboring grid cells (9 cells total: current + 8 neighbors)
//                            for (int dx = -1; dx <= 1; dx++) {
//                                for (int dy = -1; dy <= 1; dy++) {
//                                    String gridKey = (gridX + dx) + "_" + (gridY + dy);
//                                    resultt.add(new Tuple2<>(gridKey, p));
//                                }
//                            }
//
//                            return resultt.iterator();
//                        })
//                        .groupByKey()
//                        .flatMapToPair(gridGroup -> {
//                            List<Point> pts = new ArrayList<>();
//                            gridGroup._2.forEach(pts::add);
//
//                            List<Tuple2<String, String>> merges = new ArrayList<>();
//
//                            for (int i = 0; i < pts.size(); i++) {
//                                for (int j = i + 1; j < pts.size(); j++) {
//                                    Point a = pts.get(i);
//                                    Point b = pts.get(j);
//
//                                    // Must be different points from different cells
//                                    if (a.id == b.id || a.cellId == b.cellId)
//                                        continue;
//
//                                    if (!(a.isCorePoint && b.isCorePoint))
//                                        continue;
//
//                                    float dist = Utils.distance(a, b);
//                                    if (dist <= eps2) {
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
//                        });
//
//        JavaPairRDD<String, String> allMergesRDD =
//                samePointMergeRDD.union(crossCellMergeRDD).distinct();


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
        JavaPairRDD<Long, Integer> pointToGlobalId=pointToGlobal(groupedByPoint,bcMapToGlobalId);
        if (DEBUG){
            System.out.println("Point to Global ID Map");
            pointToGlobalId.take(200).forEach(pair -> System.out.println("PointTOGlobal: "+pair._1() + ": " + pair._2()));
            pointToGlobalId.coalesce(1).saveAsTextFile("output/pointToGlobalId"+ runId);
        }


//        JavaPairRDD<Long, Integer> pointToGlobalId =
//                groupedByPoint.mapValues(pts -> {
//                    // Collect ALL global IDs
//                    Map<Integer, Integer> gidCounts = new HashMap<>();
//
//                    for (Point p : pts) {
//                        if (p.clusterId > 0) {
//                            String key = p.cellId + "_" + p.clusterId;
//                            Integer gid = bcMapToGlobalId.value().get(key);
//
//                            if (gid != null && gid > 0) {
//                                gidCounts.put(gid, gidCounts.getOrDefault(gid, 0) + 1);
//                            }
//                        }
//                    }
//
//                    if (gidCounts.isEmpty()) return -1;
//
//                    if (gidCounts.size() > 1) {
//                        System.err.println("TEST: Point has multiple global IDs: " + gidCounts+"Points :"+pts);
//                    }
//
//                    // Return most frequent (or first if tie)
//                    return gidCounts.entrySet().stream()
//                            .max(Map.Entry.comparingByValue())
//                            .map(Map.Entry::getKey)
//                            .orElse(-1);
//                });



        JavaPairRDD<Long, Point> idPointPairRDD =
                dbscanClusteredAsPerCellsRDD
                        .mapToPair(p -> new Tuple2<>(p._2.id, p._2));

        if (DEBUG){
            System.out.println("idPointPairRDD Map");
            pointToGlobalId.take(50).forEach(pair -> System.out.println("idPointPairRDD: "+pair._1() + ": " + pair._2()));
        }

        JavaRDD<Point> finalClusters =
                idPointPairRDD
                        .join(pointToGlobalId)
                        .map(t -> {
                            Point p = t._2._1;
                            p.clusterId = t._2._2;
                            return p;
                        })
                        .filter(p -> p.isLocalRegion);
        long writeStart = System.currentTimeMillis();
        finalClusters.coalesce(1).saveAsTextFile("output/finalClusters"+ runId);
        long writeEnd = System.currentTimeMillis();

        long endTime = System.currentTimeMillis();
        result.runtimeMs = endTime - startTime;
        result.totalPoints = finalClusters.count();
        result.ghostPoints = ghostPoints.value();
        result.neighborQueryCount = neighborQueryCount.value();
        result.shuffleReadMBytes =sparkMetricListener.shuffleRead;
        result.shuffleWriteMBytes=sparkMetricListener.shuffleWrite;
        result.diskSpilledBytes=sparkMetricListener.diskSpilled;
        result.memorySpilledBytes=sparkMetricListener.memorySpilled;


        double T1Sec = neighborQueryTimeNs.value() / 1e9;
        double readSec = (readEnd - readStart) / 1000.0;
        double writeSec = (writeEnd - writeStart) / 1000.0;
        double T2Sec = readSec + writeSec;
        double totalSec = (endTime - startTime) / 1000.0;
        double ratio = (T1Sec / totalSec) * 100.0;

        result.neighborhoodTimeSec = T1Sec;
        result.ioTimeSec = T2Sec;
        result.totalTimeSec = totalSec;
        result.neighborhoodPercent = ratio;
        result.dataScale = totalPoints;
        result.numClusters=localToGlobal.size();
        result.noisePoints=(int)finalClusters.filter(point -> point.clusterId<0).count();

        return  result;
    }

    public static JavaRDD<Point> readPoints(JavaSparkContext sc,String inputPath){
         return sc.textFile(inputPath)
                .zipWithIndex()
                .filter(t -> !t._1.trim().isEmpty())
                .map(t -> {
                    String[] parts = t._1.trim().split(",");
                    float x = Float.parseFloat(parts[0]);
                    float y = Float.parseFloat(parts[1]);
                    return new Point(t._2, x, y, 0);
                });
    }

    public static JavaPairRDD<Long, Integer> pointToGlobal(JavaPairRDD<Long, Iterable<Point>> groupedByPoint,Broadcast<Map<String, Integer>> bcMapToGlobalId)
    {
        return groupedByPoint.mapValues(pts -> {

                Integer coreGid = null;
                Integer borderGid = null;

                for (Point p : pts) {
                    if (p.clusterId > 0) {
                        String key = p.cellId + "_" + p.clusterId;
                        Integer gid = bcMapToGlobalId.value().get(key);
                        if (gid == null) continue;

                        if (p.isCorePoint)
                            coreGid = (coreGid == null) ? gid : Math.min(coreGid, gid);
                        else
                            borderGid = (borderGid == null) ? gid : Math.min(borderGid, gid);
                    }
                }
                if (coreGid != null) return coreGid;
                if (borderGid != null) return borderGid;
                return -1;
            });
    }


    public static JavaPairRDD<String, String>  samePointMerge(JavaPairRDD<Long, Iterable<Point>> groupedByPoint){
         return groupedByPoint.flatMapToPair(entry -> {

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

                            String keyA = a.cellId + "_" + a.clusterId;
                            String keyB = b.cellId + "_" + b.clusterId;
                            if (!keyA.equals(keyB))
                                merges.add(new Tuple2<>(keyA, keyB));
                        }
                    }
                    return merges.iterator();
                });
    }

    public static JavaPairRDD<Integer, Point> dbscanClusteredAsPerCells(JavaPairRDD<Integer, Point> partitionedToCellsRDD,
            float eps2, int minPts, LongAccumulator neighborQueryCount, LongAccumulator neighborQueryTimeNs) {

        return partitionedToCellsRDD
                .groupByKey()
                .flatMapToPair(cell -> {

                    int cellId = cell._1;
                    List<Point> cellPoints = new ArrayList<>();
                    cell._2.forEach(cellPoints::add);

                    // Local DBSCAN inside one cell
                    Utils.localDBSCAN(
                            cellPoints,
                            eps2,
                            minPts,
                            neighborQueryCount,
                            neighborQueryTimeNs
                    );

                    List<Tuple2<Integer, Point>> out = new ArrayList<>();
                    for (Point p : cellPoints) {
                        out.add(new Tuple2<>(cellId, p));
                    }
                    return out.iterator();
                })
                .cache();
    }


    public static JavaPairRDD<Integer, Point> partitionPointsToCells(JavaRDD<Point> points, float minLatitude, float minLongitude,
            Broadcast<PartitionConfiguration> broadcastPartitionConf, LongAccumulator ghostPoints, float EPS) {

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
            if (homeX > 0 && dxLeft <= cfg.buffer+EPS) {
                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX - 1),ghostPoints);
            }
            // Right Neighbor
            if (homeX < cfg.numCellsX - 1 && dxRight <= cfg.buffer+EPS) {
                addGhost(assignments, p, homeY * cfg.numCellsX + (homeX + 1),ghostPoints);
            }
            // Bottom Neighbor
            if (homeY > 0 && dyBottom <= cfg.buffer+EPS) {
                addGhost(assignments, p, (homeY - 1) * cfg.numCellsX + homeX,ghostPoints);
            }
            // Top Neighbor
            if (homeY < cfg.numCellsY - 1 && dyTop <= cfg.buffer+EPS) {
                addGhost(assignments, p, (homeY + 1) * cfg.numCellsX + homeX,ghostPoints);
            }

            // Top-Left (homeX-1, homeY+1)
            if (homeX > 0 && homeY < cfg.numCellsY - 1 && dxLeft <= cfg.buffer+EPS && dyTop <= cfg.buffer+EPS) {
                int cellId = (homeY + 1) * cfg.numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId,ghostPoints);
            }

            // Top-Right (homeX+1, homeY+1)
            if (homeX < cfg.numCellsX - 1 && homeY < cfg.numCellsY - 1 && dxRight <= cfg.buffer+EPS && dyTop <= cfg.buffer+EPS) {
                int cellId = (homeY + 1) * cfg.numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId,ghostPoints);
            }

            // Bottom-Left (homeX-1, homeY-1)
            if (homeX > 0 && homeY > 0 && dxLeft <= cfg.buffer+EPS && dyBottom <= cfg.buffer+EPS) {
                int cellId = (homeY - 1) * cfg.numCellsX + (homeX - 1);
                addGhost(assignments, p, cellId,ghostPoints);
            }

            // Bottom-Right (homeX+1, homeY-1)
            if (homeX < cfg.numCellsX - 1 && homeY > 0 && dxRight <= cfg.buffer+EPS && dyBottom <= cfg.buffer+EPS) {
                int cellId = (homeY - 1) * cfg.numCellsX + (homeX + 1);
                addGhost(assignments, p, cellId,ghostPoints);
            }

            if (p.latitude == 0.6f && p.longitude == 0.45f) {
                System.out.println("DEBUG (0.60,0.45)");
                System.out.println("homeX=" + homeX + ", homeY=" + homeY);
                System.out.println("dxRight=" + dxRight + ", buffer=" + cfg.buffer);
                System.out.println("homeX < numCellsX-1 = " + (homeX < cfg.numCellsX - 1));
            }
            return assignments.iterator();
        });

        return partitionedToCellsRDD;
    }




    private static void addGhost(List<Tuple2<Integer, Point>> list, Point original, int targetCellId, LongAccumulator ghostPoints) {
        Point ghost = new Point(original.id, original.latitude, original.longitude, 0);
        ghost.cellId = targetCellId;
        ghost.isLocalRegion = false;
        list.add(new Tuple2<>(targetCellId, ghost));
        ghostPoints.add(1);
    }
}
