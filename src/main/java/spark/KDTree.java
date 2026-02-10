package spark;

import org.apache.spark.util.LongAccumulator;

import java.util.*;

public class KDTree {
    static class Node {
        Point point;
        Node left, right;
        int dimension;


        Node(Point p, int dimension) {
            this.point = p;
            this.dimension = dimension;
            this.left = this.right = null;
        }
    }

    private final Node root;

    public KDTree(List<Point> points) {
        this.root = build(new ArrayList<>(points), 0);
    }

    private Node build(List<Point> pts, int depth) {
        if (pts.isEmpty()) return null;

        int axis = depth % pts.get(0).dimensions;

        pts.sort(Comparator.comparingDouble(
                p -> p.coordinates[axis]
        ));

        int mid = pts.size() / 2;

        Node node = new Node(pts.get(mid), axis);

        node.left  = build(pts.subList(0, mid), depth + 1);
        node.right = build(pts.subList(mid + 1, pts.size()), depth + 1);

        return node;
    }

    public List<Point> radiusSearch(Point target, double eps2,QueryMetrics metrics) {
        long start = System.nanoTime();

        List<Point> neighbours = new ArrayList<>();
        radiusSearch(root, target, eps2, neighbours);
        metrics.incrementQueryCount();
        metrics.addQueryTime(System.nanoTime() - start);

        return neighbours;
    }

    private void radiusSearch(Node node, Point t, double eps2, List<Point> neighbours) {
        if (node == null) return;

        if (distanceSquared(node.point, t) <= eps2) {
            neighbours.add(node.point);
        }

        int axis = node.dimension;
        double diff = t.coordinates[axis] - node.point.coordinates[axis];

        Node near = diff <= 0 ? node.left : node.right;
        Node far  = diff <= 0 ? node.right : node.left;

        radiusSearch(near, t, eps2, neighbours);

        if (diff * diff <= eps2 ) {
            radiusSearch(far, t, eps2, neighbours);
        }
    }


    public static double distanceSquared(Point a, Point b) {
        double sum = 0.0;
        for (int i = 0; i < a.dimensions; i++) {
            double diff = a.coordinates[i] - b.coordinates[i];
            sum += diff * diff;
        }
        return sum;
    }

    //     Build for 2D coordinate system
//    private Node build(List<Point> pts, int depth) {
//        if (pts.isEmpty()) return null;
//        int dimension = depth % 2;
//        pts.sort(Comparator.comparing(p -> dimension == 0 ? p.latitude : p.longitude, Float::compare));
//        int mid = pts.size() / 2;
//
//        Node node = new Node(pts.get(mid), dimension);
//        //System.out.println(node.point+" "+node.dimension);
//        node.left  = build(pts.subList(0, mid), depth + 1);
//        node.right = build(pts.subList(mid + 1, pts.size()), depth + 1);
//        return node;
//    }


//    private void radiusSearch(Node node, Point t, float eps2, List<Point> neighbours) {
//        if (node == null) return;
//        if (distance(node.point, t) <= eps2+EPS) neighbours.add(node.point);
//
//        int axis = node.dimension;
//        float diff = (axis == 0 ? t.latitude - node.point.latitude : t.longitude - node.point.longitude);
//
//        Node near = diff <= 0 ? node.left : node.right;
//        Node far  = diff <= 0 ? node.right : node.left;
//
//        radiusSearch(near, t, eps2,  neighbours);
//
//        if (diff * diff <= eps2) {
//            radiusSearch(far, t, eps2, neighbours);
//        }
//    }
    

//    private float distance(Point a, Point b) {
//        float dx = a.latitude - b.latitude;
//        float dy = a.longitude - b.longitude;
//        return dx*dx + dy*dy;
//    }





    public static void main(String[] args) {

        List<Point> points = List.of(
                new Point(0,new double[]{2.0, 3.0},0),
                new Point(0,new double[]{5, 4},0),
                new Point(0,new double[]{9, 6},0),
                new Point(0,new double[]{4, 7},0),
                new Point(0,new double[]{8, 1},0),
                new Point(0,new double[]{7, 2},0)
        );

        KDTree tree = new KDTree(points);

        Point query = new Point(0,new double[]{5, 5},0);
        float eps = 3.0f;

        List<Point> neighbors = tree.radiusSearch(query, eps, null);

        System.out.println("Query point: " + query);
        System.out.println("Radius eps = " + eps);
        System.out.println("Radius eps = " + eps);
        System.out.println("Neighbors:");

        for (Point p : neighbors) {
            System.out.println("  " + p);
        }
    }
}

