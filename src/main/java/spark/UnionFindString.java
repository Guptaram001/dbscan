package spark;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class UnionFindString {
    Map<String, String> parent = new HashMap<>();

    String find(String x) {
        parent.putIfAbsent(x, x);
        if (!parent.get(x).equals(x)) {
            parent.put(x, find(parent.get(x)));
        }
        return parent.get(x);
    }

    void union(String a, String b) {
        String pa = find(a);
        String pb = find(b);
        if (!pa.equals(pb)) parent.put(pa, pb);
    }
    public static void main(String[] args) {
        UnionFindString unionFindString = new UnionFindString();
        unionFindString.union("12_1", "13_2");
        unionFindString.union("13_2", "14_1");
        unionFindString.union("20_1", "21_1");
        unionFindString.union("12_1", "15_1");

        System.out.println("Union Find String: After Insertion" );
        System.out.println(unionFindString.parent);
        System.out.println("Total KeySet: "+unionFindString.parent.keySet().size()+" And Values:  "+unionFindString.parent.keySet());

        Map<String, String> keyToRoot = new HashMap<>();
        for (String k : unionFindString.parent.keySet()) {
            keyToRoot.put(k, unionFindString.find(k));
        }

        System.out.println("Union Find String: After KeyToRoot Mapping" );
        System.out.println(unionFindString.parent);
        System.out.println("keyToRoot: "+keyToRoot);

        Map<String, Integer> rootToGlobal = new HashMap<>();
        int globalId = 1;
        for (String root : new HashSet<>(keyToRoot.values())) {
            rootToGlobal.put(root, globalId++);
        }
        System.out.println("Finding the root and assigning each root Global Id: "+rootToGlobal);


        Map<String, Integer> localToGlobal = new HashMap<>();
        for (Map.Entry<String, String> e : keyToRoot.entrySet()) {
            localToGlobal.put(e.getKey(), rootToGlobal.get(e.getValue()));
        }

        System.out.println("Mapping of the keys from keyRoot to Global: "+localToGlobal);

    }
}
