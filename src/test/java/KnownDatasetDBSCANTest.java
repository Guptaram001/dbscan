
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.*;
import java.nio.file.*;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import spark.*;

public class KnownDatasetDBSCANTest {

    private JavaSparkContext sc;

    @BeforeEach
    void setup() throws Exception {
        Files.createDirectories(Paths.get("results"));
        Files.createDirectories(Paths.get("output"));

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KnownDatasetTest")
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

    @Test
    void expectedClustersAndNoiseTest() throws Exception {
        URL resource = getClass()
                .getClassLoader()
                .getResource("test.csv");

        assertNotNull(resource, "test.csv not found in test resources");

        Path input = Paths.get(resource.toURI());
        ExecutionConfiguration cfg = new ExecutionConfiguration(
                1.5f,
                3,
                2.0f,
                1.0f,
                "UF",
                false,
                input.toString()
        );

        SparkMetricListener metrics = new SparkMetricListener();
        sc.sc().addSparkListener(metrics);
        String runId = String.valueOf(System.currentTimeMillis());

        Result result = ExecuteDBSCAN.executeDBSCAN(sc, cfg, metrics,runId);


        assertEquals(37, result.dataScale, "Total points mismatch");

        assertEquals(6, result.numClusters,
                "Expected exactly 6 clusters");

        assertEquals(4, result.noisePoints,
                "Expected exactly 4 noise points");
    }

//    @Test
//    void partitionPointsToCellsTest() throws Exception {
//
//        Path input = Paths.get(
//                Objects.requireNonNull(
//                        getClass().getClassLoader().getResource("test.csv")
//                ).toURI()
//        );
//
//        Path expectedFile = Paths.get(
//                Objects.requireNonNull(
//                        getClass().getClassLoader()
//                                .getResource("expected_partitioned_to_cells.txt")
//                ).toURI()
//        );
//
//        List<String> expected =
//                Files.readAllLines(expectedFile)
//                        .stream()
//                        .map(String::trim)
//                        .sorted()
//                        .collect(Collectors.toList());
//
//        JavaRDD<Point> points =
//                ExecuteDBSCAN.readPoints(sc, input.toString()).cache();
//
//        float minLat = points.map(p -> p.latitude).reduce(Float::min);
//        float maxLat = points.map(p -> p.latitude).reduce(Float::max);
//        float minLon = points.map(p -> p.longitude).reduce(Float::min);
//        float maxLon = points.map(p -> p.longitude).reduce(Float::max);
//
//        PartitionConfiguration pc =
//                new PartitionConfiguration(minLat, maxLat, minLon, maxLon,
//                        1.5f, 3f, 1f);
//
//        Broadcast<PartitionConfiguration> bc = sc.broadcast(pc);
//        LongAccumulator ghostPoints = sc.sc().longAccumulator();
//        final float EPS = 1e-6f;
//
//        List<String> actual =
//                ExecuteDBSCAN.partitionPointsToCells(
//                                points, minLat, minLon, bc, ghostPoints,EPS)
//                        .map(t -> t._1 + "," + t._2.toString())
//                        .collect()
//                        .stream()
//                        .map(String::trim)
//                        .sorted()
//                        .collect(Collectors.toList());
//
//        assertEquals(expected, actual);
//        assertTrue(ghostPoints.value() > 0);
//    }
}
