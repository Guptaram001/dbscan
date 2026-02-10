package spark;

import algebra.lattice.Bool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class EntryPoint {

    public static void main(String[] args) throws Exception {
        Result res;
        String mode = args.length > 1 ? args[1] : "Serial";

        List<ExecutionConfiguration> tests = List.of(
                // new ExecutionConfiguration(0.03f, 50, 3, 1,"UF", true,args[0])
                //new ExecutionConfiguration(0.1f, 2, 3, 1,"UF", true,args[0])
                new ExecutionConfiguration(0.03f, 50, 3, 0.1f,mode, Boolean.parseBoolean(args[7]),args[0])

        );

        Files.createDirectories(Paths.get("results"));
        for (ExecutionConfiguration executionConfiguration : tests) {
            String runId = String.valueOf(System.currentTimeMillis());
            Files.createDirectories(Paths.get("results/Exec_"+runId));
            FileWriter out = new FileWriter("results/Exec_"+runId+"/results.txt");

            SparkConf conf = new SparkConf()
                    .setAppName("DBSCAN-" + executionConfiguration.formId())
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.kryo.registrator", "spark.MyRegistrator")
                    .set("spark.kryo.registrationRequired", "false");

            JavaSparkContext sc = new JavaSparkContext(conf);

            SparkMetricListener sparkMetricListener = new SparkMetricListener();
            sc.sc().addSparkListener(sparkMetricListener);

            res = ExecuteDBSCAN.executeDBSCAN(sc, executionConfiguration, sparkMetricListener,runId);
            res.dataset=args[0];
            res.totalWorkerMemory=Integer.parseInt(args[2].split("g")[0]);
            res.noWorkerCores=Integer.parseInt(args[3]);
            res.noWorkers=Integer.parseInt(args[4]);
            res.driverMemory=Integer.parseInt(args[2].split("g")[0]);
            res.driverCores=Integer.parseInt(args[6]);

            System.out.println(res.toString());
            out.write(res.printHeader() + "\n");
            out.write(res.toString() + "\n");
            out.flush();
            out.close();
            sc.stop();
        }

    }
}
