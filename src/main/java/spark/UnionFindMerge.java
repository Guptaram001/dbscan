package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class UnionFindMerge {

    public JavaPairRDD<String, Integer> merge (JavaSparkContext sc, JavaPairRDD<String, String> samePointMergeRDD, JavaPairRDD<Integer, Point> clusteredRDD, boolean DEBUG) {

        //Gets all merges to the driver
        List<Tuple2<String, String>> samePointMergeRDDList = samePointMergeRDD.collect();

        UnionFindString uf = new UnionFindString();
        for (Tuple2<String, String> e : samePointMergeRDDList) {
            uf.union(e._1, e._2);
        }
        if (DEBUG) {
            System.out.println("Union Find String: After Insertion");
            System.out.println(uf.parent);
            System.out.println("Total KeySet: " + uf.parent.keySet().size() + " And Values:  " + uf.parent.keySet());
        }
        Map<String, String> keyToRoot = new HashMap<>();
        for (String k : uf.parent.keySet()) {
            keyToRoot.put(k, uf.find(k));
        }
        if (DEBUG) {
            System.out.println("Union Find String: After Merged KeyToRoot Mapping" );
            System.out.println(uf.parent);
            System.out.println("After Merged KeyToRoot keyToRoot: "+keyToRoot);
        }

        clusteredRDD.values()
                .mapPartitions(iter -> {

                    Set<String> local = new HashSet<>();

                    while (iter.hasNext()) {
                        Point p = iter.next();
                        if (p.clusterId > 0) {
                            local.add(p.cellId + "_" + p.clusterId);
                        }
                    }
                    return local.iterator();
                })
                .collect()
                .forEach(k -> keyToRoot.putIfAbsent(k, k));

        if (DEBUG) {
            System.out.println("Union Find String: After Adding Isolated Clusters" );
            System.out.println(uf.parent);
            System.out.println("After Including Isolated too KeyToRoot: "+keyToRoot);
        }

        Map<String, Integer> rootToGlobal = new HashMap<>();
        int globalId = 1;
        for (String root : new HashSet<>(keyToRoot.values())) {
            rootToGlobal.put(root, globalId++);
        }
        if (DEBUG)
            System.out.println("Finding the root and assigning each root Global Id: "+rootToGlobal);


        JavaPairRDD<String, Integer> edgesToGlobal = sc.parallelize(new ArrayList<>(keyToRoot.entrySet()))
                            .mapToPair(e ->
                                    new Tuple2<>(e.getKey(), rootToGlobal.get(e.getValue())));

//            JavaPairRDD<String, Integer> edgesToGlobal = new HashMap<>();
//            for (Map.Entry<String, String> e : keyToRoot.entrySet()) {
//                edgesToGlobal.put(e.getKey(), rootToGlobal.get(e.getValue()));
//                out.write(e.getKey()+","+ rootToGlobal.get(e.getValue())+"\n");
//            }
            return edgesToGlobal;
        }

    }

