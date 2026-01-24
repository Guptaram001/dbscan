package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

public class GraphxMerge {
    public Map<String, Integer> merge(JavaPairRDD<String, String> samePointMergeRDD, JavaPairRDD<Integer, Point> clusteredRDD){

        //All local clusters
        JavaRDD<String> allClusters = clusteredRDD.values()
                .filter(p -> p.clusterId > 0)
                .map(p -> p.cellId + "_" + p.clusterId)
                .distinct()
                .cache();
        System.out.println("Total local clusters (vertices): " + allClusters.count());

        //Assign Id to every local cluster label
        JavaPairRDD<String, Long> assignClusterId = allClusters.zipWithIndex()
                .mapToPair(t -> new Tuple2<>(t._1, t._2))
                .cache();

        // vertices: (id, attr). attr can be anything
        JavaRDD<scala.Tuple2<Object, Long>> vertices = assignClusterId
                .map(t -> new scala.Tuple2<Object, Long>(t._2, t._2));

        // Create the both direction of edges
        JavaPairRDD<String, String> undirectedEdges = samePointMergeRDD.flatMapToPair(e ->
                Arrays.asList(
                        new Tuple2<>(e._1, e._2),
                        new Tuple2<>(e._2, e._1)
                ).iterator()
        ).distinct();

        // Map src label -> src id
        JavaPairRDD<String, Tuple2<String, Long>> srcWithId = undirectedEdges
                .join(assignClusterId);  // (srcLabel, (dstLabel, srcId))

        // Key by dstLabel to join dst id
        JavaPairRDD<String, Tuple2<String, Long>> keyedByDst = srcWithId
                .mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2)));
        // (dstLabel, (srcLabel, srcId))

        // (srcID,  dstId,1)
        JavaRDD<Edge<Long>> edges = keyedByDst
                .join(assignClusterId) // (dstLabel, ((srcLabel, srcId), dstId))
                .map(t -> new Edge<>(t._2._1._2, t._2._2, 1L))
                .distinct();
        System.out.println("Graph edges: " + edges.count());

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

        // vertexToComponent: (id - componentId)
        JavaPairRDD<Long, Long> idToComp = cc.vertices().toJavaRDD()
                .mapToPair(t -> new Tuple2<>((Long) t._1, (Long) t._2))
                .cache();

        // Convert back: (label - componentId)
        JavaPairRDD<Long, String> idToLabel = assignClusterId.mapToPair(t -> new Tuple2<>(t._2, t._1));

        // (id, (label, comp))
        JavaPairRDD<String, Long> labelToComp = idToLabel
                .join(idToComp)
                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
                .cache();


        // Assign sequential global IDs to each component
        JavaPairRDD<Long, Integer> compToGlobal = labelToComp.values()
                .distinct()
                .zipWithIndex()
                .mapToPair(t -> new Tuple2<>(t._1, (int) (t._2 + 1)));

        JavaPairRDD<String, Integer> localToGlobal = labelToComp
                .mapToPair(t -> new Tuple2<>(t._2, t._1))   // (comp, label)
                .join(compToGlobal)    // (comp, (label, gid))
                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2));
        Map<String,Integer> map = new HashMap<>();
        map=localToGlobal.collectAsMap();

        return map;
    }
}

