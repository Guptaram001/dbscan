
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.*;
import spark.*;

import java.io.IOException;
import java.nio.file.*;
import static org.junit.jupiter.api.Assertions.*;

public class GhostReplicationTest {

    private JavaSparkContext sc;

    @BeforeEach
    void setup() throws IOException {
        Files.createDirectories(Paths.get("results"));
        Files.createDirectories(Paths.get("output"));

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

    @Test
    void createsGhostPointsTest() throws IOException {
        Path tmp = Files.createTempFile("ghosts_", ".csv");
        Files.writeString(tmp,
                "0.1,0.1\n" +
                        "1.9,0.1\n" +
                        "2.1,0.1\n" +
                        "4.0,4.0\n");

        ExecutionConfiguration cfg = new ExecutionConfiguration(
                1.0f,
                2,
                2.0f,
                1.0f,
                "UF",
                false,
                tmp.toString()
        );

        SparkMetricListener metrics = new SparkMetricListener();
        sc.sc().addSparkListener(metrics);
        String runId = String.valueOf(System.currentTimeMillis());

        Result res = ExecuteDBSCAN.executeDBSCAN(sc, cfg, metrics,runId);

        assertTrue(res.ghostPoints > 0, "Expected some ghost points due to boundary replication");
    }
}
