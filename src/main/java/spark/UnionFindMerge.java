package spark;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class UnionFindMerge {

    public Map<String, Integer> merge (JavaPairRDD<String, String> samePointMergeRDD, JavaPairRDD<Integer, Point> clusteredRDD) {

        List<Tuple2<String, String>> samePointMergeRDDList = samePointMergeRDD.collect();

        UnionFindString uf = new UnionFindString();
        for (Tuple2<String, String> e : samePointMergeRDDList) {
            uf.union(e._1, e._2);
        }


        System.out.println("Union Find String: After Insertion" );
        System.out.println(uf.parent);
        System.out.println("Total KeySet: "+uf.parent.keySet().size()+" And Values:  "+uf.parent.keySet());

        Map<String, String> keyToRoot = new HashMap<>();
        for (String k : uf.parent.keySet()) {
            keyToRoot.put(k, uf.find(k));
        }

        System.out.println("Union Find String: After Merged KeyToRoot Mapping" );
        System.out.println(uf.parent);
        System.out.println("After Merged KeyToRoot keyToRoot: "+keyToRoot);

        Set<String> allKeys = new HashSet<>(
                clusteredRDD.values()
                        .filter(p -> p.clusterId > 0)
                        .map(p -> p.cellId + "_" + p.clusterId)
                        .distinct()
                        .collect()
        );

        for (String s: allKeys) {
                keyToRoot.putIfAbsent(s, s);
        }

        System.out.println("Union Find String: After Adding Isolated Clusters" );
        System.out.println(uf.parent);
        System.out.println("After Including Isolated too KeyToRoot: "+keyToRoot);

        Map<String, Integer> rootToGlobal = new HashMap<>();
        int globalId = 1;
        for (String root : new HashSet<>(keyToRoot.values())) {
            rootToGlobal.put(root, globalId++);
        }
        System.out.println("Finding the root and assigning each root Global Id: "+rootToGlobal);

        FileWriter out = null;
        try {
            out = new FileWriter("results/edgesToGlobal.csv");

            Map<String, Integer> edgesToGlobal = new HashMap<>();
            for (Map.Entry<String, String> e : keyToRoot.entrySet()) {
                edgesToGlobal.put(e.getKey(), rootToGlobal.get(e.getValue()));
                out.write(e.getKey()+","+ rootToGlobal.get(e.getValue())+"\n");
            }
            System.out.println("Mapping of the keys from keyRoot to Global: " + edgesToGlobal);
            out.flush();
            out.close();
            return edgesToGlobal;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
