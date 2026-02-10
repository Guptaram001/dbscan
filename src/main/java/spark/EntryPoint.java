package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class EntryPoint {

    public static void main(String[] args) throws Exception {
        Result res;
//        List<ExecutionConfiguration> experiments = List.of(
//                new ExecutionConfiguration(0.02, 30, 3, 1,"UF", args[0]),
//                new ExecutionConfiguration(0.03, 50, 3.0, 1,"UF", args[0]),
//                new ExecutionConfiguration(0.03, 50, 3.0, 1,"GraphX", args[0]),
//                new ExecutionConfiguration(0.03, 50, 3.0, 1,"SerialDBSCAN", args[0]),
//        );

        List<ExecutionConfiguration> tests = List.of(
               // new ExecutionConfiguration(0.03f, 50, 3, 1,"UF", true,args[0])
                 //new ExecutionConfiguration(0.1f, 2, 3, 1,"UF", true,args[0])
                new ExecutionConfiguration(0.03f, 70, 3, 1,"UF", true,args[0])

        );
        System.out.println("Running " + tests.size() + " tests"+args[0]);

<<<<<<< Updated upstream
        Files.createDirectories(Paths.get("results"));
        FileWriter out = new FileWriter("results/results.csv");
=======
        FileWriter out = new FileWriter("results/results.csv", true);

        boolean headerWritten = false;
>>>>>>> Stashed changes

        for (ExecutionConfiguration executionConfiguration : tests) {

//            SparkConf conf = new SparkConf()
//                    .setAppName("DBSCAN-" + executionConfiguration.formId());
            SparkConf conf = new SparkConf()
                    .setAppName("DBSCAN-" + executionConfiguration.formId())
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.kryo.registrator", "spark.MyRegistrator")
                    .set("spark.kryo.registrationRequired", "false");

            JavaSparkContext sc = new JavaSparkContext(conf);

            SparkMetricListener sparkMetricListener = new SparkMetricListener();
            sc.sc().addSparkListener(sparkMetricListener);

            if(executionConfiguration.mergeStrategy.equals("SerialDBSCAN")){
                res = SerialDBSCAN.executeDBSCAN( executionConfiguration);
            }else {
                res = ExecuteDBSCAN.executeDBSCAN(sc, executionConfiguration, sparkMetricListener);
            }

            System.out.println(res.toString());
            if (!headerWritten) {
                out.write(res.printHeader() + "\n");
                headerWritten = true;
            }
            out.write(res.toString() + "\n");
            out.flush();

            sc.stop();
        }

        out.close();
    }
}
