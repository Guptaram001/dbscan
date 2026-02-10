package spark;

import org.apache.spark.util.LongAccumulator;
import java.util.*;

public class Utils {
    public static void localDBSCAN(List<Point> points, double eps2, int minPts,QueryMetrics metrics) {

        //Map<Float, Boolean> visited = new HashMap<>();
        BitSet visited = new BitSet(points.size());
        int localClusterId = 0;
        int i = 0;
        KDTree kdTree=new KDTree(points);
        for (Point p : points) {
            if (++i % 1000 == 0) {
                System.out.println("Processed " + i + " / " + points.size());
            }
            if (visited.get((int)p.id)) {
                continue;
            }

            visited.set((int)p.id);
            //List<Point> neighbors = regionQuery(points, p, eps, queryCount, queryTime);
            List<Point> neighbors =kdTree.radiusSearch(p,eps2,metrics);

            if (neighbors.size() < minPts) {
                p.clusterId = -1;
                p.isCorePoint = false;
            } else {
                p.isCorePoint = true;
                localClusterId++;
                expandCluster(kdTree, p, neighbors, localClusterId, eps2, minPts, visited, metrics);
            }
        }
    }

    public static void expandCluster(KDTree kdTree, Point p, List<Point> neighbors,
                                     int clusterId, double eps2, int minPts, BitSet visited,
                                     QueryMetrics metrics) {
        p.clusterId = clusterId;

        Queue<Point> seeds = new LinkedList<>(neighbors);

        while (!seeds.isEmpty()) {
            Point q = seeds.poll();

            if (!visited.get((int)q.id)) {
                visited.set((int)q.id);
                //List<Point> qNeighbors = regionQuery(points, q, eps, queryCount, queryTime);

                List<Point> qNeighbors =kdTree.radiusSearch(q,eps2,metrics);
                if (qNeighbors.size() >= minPts) {
                    q.isCorePoint = true;
                    for (Point qn : qNeighbors) {
                        if (!visited.get((int)qn.id)) {
                            seeds.add(qn);
                        }
                    }
                }
            }

            if (q.clusterId <= 0) {
                q.clusterId = clusterId;
            }
        }
    }

//    public static float distance(Point a, Point b) {
//        float dx = a.latitude - b.latitude;
//        float dy = a.longitude - b.longitude;
//        return dx * dx + dy * dy;
//    }


//    public static double distanceSquared(Point a, Point b) {
//        double sum = 0.0;
//        for (int i = 0; i < a.dimensions; i++) {
//            double diff = a.coordinates[i] - b.coordinates[i];
//            sum += diff * diff;
//        }
//        return sum;
//    }

//    public static List<Point> regionQuery(List<Point> points, Point p, float eps2, LongAccumulator queryCount, LongAccumulator queryTime) {
//        long start = System.nanoTime();
//
//        List<Point> neighbors = new ArrayList<>();
//        for (Point q : points) {
//            if (distanceSquared(p, q) <= eps2 ) {
//                neighbors.add(q);
//            }
//        }
//
//        long end = System.nanoTime();
//        queryTime.add(end - start);
//        queryCount.add(1);
//
//        return neighbors;
//    }


}
