package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileWriter;
import java.util.List;

public class EntryPoint {

    public static void main(String[] args) throws Exception {

//        List<ExecutionConfiguration> experiments = List.of(
//                new ExecutionConfiguration(0.02, 30, 3, 1,"UF", args[0]),
//                new ExecutionConfiguration(0.03, 50, 3.0, 1,"UF", args[0]),
//                new ExecutionConfiguration(0.03, 50, 3.0, 1,"GraphX", args[0])
//        );

        List<ExecutionConfiguration> experiments = List.of(
                new ExecutionConfiguration(0.03f, 50, 3, 1,"GraphX", args[0])
        );

        FileWriter out = new FileWriter("results/results.csv");

        for (ExecutionConfiguration executionConfiguration : experiments) {

            SparkConf conf = new SparkConf()
                    .setAppName("DBSCAN-" + executionConfiguration.formId());

            JavaSparkContext sc = new JavaSparkContext(conf);

            SparkMetricListener sparkMetricListener = new SparkMetricListener();
            sc.sc().addSparkListener(sparkMetricListener);

            Result res = ExecuteDBSCAN.executeDBSCAN(sc, executionConfiguration, sparkMetricListener);
            System.out.println(res.toString());
            out.write(res.printHeader()+ "\n");
            out.write(res.toString() + "\n");
            out.flush();

            sc.stop();
        }

        out.close();
    }
}
