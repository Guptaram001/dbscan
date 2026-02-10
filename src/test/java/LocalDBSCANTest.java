
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import spark.*;

public class LocalDBSCANTest {

    private JavaSparkContext sc;

    @BeforeEach
    void setup() {
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
//    void localDBSCANTest() {
//        List<Point> pts = new ArrayList<>(List.of(
//                new Point(0, 0.0f, 0.0f, 0),
//                new Point(1, 0.1f, 0.1f, 0),
//                new Point(2, 0.2f, 0.0f, 0),
//
//                new Point(3, 10.0f, 10.0f, 0),
//                new Point(4, 10.1f, 10.1f, 0),
//                new Point(5, 10.2f, 10.0f, 0),
//
//                new Point(6, 50.0f, 50.0f, 0) // noise
//        ));
//
//        LongAccumulator qc = sc.sc().longAccumulator("qc");
//        LongAccumulator qt = sc.sc().longAccumulator("qt");
//
//        float eps = 0.5f;
//        float eps2 = eps * eps;
//
//        Utils.localDBSCAN(pts, eps2, 2, qc, qt);
//
//        Point noise = pts.stream().filter(p -> p.id == 6).findFirst().orElseThrow();
//        assertEquals(-1, noise.clusterId);
//
//        for (long id : List.of(0L,1L,2L,3L,4L,5L)) {
//            Point p = pts.stream().filter(x -> x.id == id).findFirst().orElseThrow();
//            assertTrue(p.clusterId > 0, "Point " + id + " should be clustered");
//        }
//
//        assertTrue(qc.value() > 0, "Should have performed neighbor queries");
//        assertTrue(qt.value() > 0, "Should have measured query time");
//    }
}
