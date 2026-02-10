
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.*;
import scala.Tuple2;
import spark.*;


import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class UnionFindMergeTest {

    private JavaSparkContext sc;

    @BeforeEach
    void setup() throws IOException {
        Files.createDirectories(Paths.get("results"));
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("DBSCAN-Test")
                .set("spark.driver.bindAddress", "127.0.0.1")
                .set("spark.driver.host", "127.0.0.1")
                .set("spark.ui.enabled", "false")
                .set("spark.eventLog.enabled", "false");
        sc = new JavaSparkContext(conf);
    }

    @AfterEach
    void tearDown() {
        if (sc != null) sc.stop();
    }

//    @Test
//    void unionFindMergeTest() {
//        // Merge edges: "0_1" <-> "1_2"
//        List<Tuple2<String, String>> edges = List.of(
//                new Tuple2<>("0_1", "1_2")
//        );
//        JavaPairRDD<String, String> samePointMergeRDD = sc.parallelizePairs(edges);
//
//        // clusteredRDD contains 3 cluster keys: 0_1, 1_2, 2_1 (isolated)
//        List<Tuple2<Integer, Point>> clustered = List.of(
//                new Tuple2<>(0, pWithCluster(100, 0, 0, 1, 0)),
//                new Tuple2<>(1, pWithCluster(101, 0, 0, 2, 1)),
//                new Tuple2<>(2, pWithCluster(102, 0, 0, 1, 2))
//        );
//        JavaPairRDD<Integer, Point> clusteredRDD = sc.parallelizePairs(clustered);
//
//        UnionFindMerge m = new UnionFindMerge();
//        Map<String, Integer> localToGlobal = m.merge(samePointMergeRDD, clusteredRDD);
//
//        assertTrue(localToGlobal.containsKey("0_1"));
//        assertTrue(localToGlobal.containsKey("1_2"));
//        assertTrue(localToGlobal.containsKey("2_1"));
//
//        int gidA = localToGlobal.get("0_1");
//        int gidB = localToGlobal.get("1_2");
//        int gidIso = localToGlobal.get("2_1");
//
//        assertEquals(gidA, gidB, "Merged clusters should share same global id");
//        assertNotEquals(gidA, gidIso, "Isolated cluster should have different global id");
//    }

//    private static Point pWithCluster(long id, float x, float y, int clusterId, int cellId) {
//        Point p = new Point(id, x, y, clusterId);
//        p.cellId = cellId;
//        p.isLocalRegion = true;
//        p.isCorePoint = true;
//        return p;
//    }
}
