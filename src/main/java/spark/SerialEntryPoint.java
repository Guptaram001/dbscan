package spark;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SerialEntryPoint {

    public static void main(String[] args) throws Exception {

        System.out.println("ENTERED SerialEntryPoint");
        System.out.println("Args length = " + args.length);
        Result res;
        String mode = args.length > 1 ? args[1] : "Serial";
        List<ExecutionConfiguration> tests = List.of(
                new ExecutionConfiguration(0.03f, 70, 3, 1,mode, true,args[0])
        );

        Files.createDirectories(Paths.get("results"));
        for (ExecutionConfiguration executionConfiguration : tests) {
            System.out.println("Starting Serial DBSCAN...");
            System.out.println("Input file = " + args[0]);
            System.out.println("Mode = " + mode);

            String runId = String.valueOf(System.currentTimeMillis());
            System.out.println("Creating directory: results/Exec_" + runId);
            Files.createDirectories(Paths.get("results/Exec_" + runId));

            String outputPath = "results/Exec_" + runId + "/results.txt";
            System.out.println("Output file will be: " + outputPath);

            try (FileWriter out = new FileWriter(outputPath)) {
                System.out.println("Calling SerialDBSCAN.executeDBSCAN...");
                res = SerialDBSCAN.executeDBSCAN(executionConfiguration, runId);

                res.dataset=args[0];
                res.totalWorkerMemory=Integer.parseInt(args[2].split("g")[0]);
                res.noWorkerCores=Integer.parseInt(args[3]);
                res.noWorkers=0;
                res.driverMemory=0;
                res.driverCores=0;

                System.out.println("Results generated: " + res.toString());

                out.write(res.printHeader() + "\n");
                out.write(res.toString() + "\n");
                out.flush();

                System.out.println("✓ Results written to: " + outputPath);
            } catch (Exception e) {
                System.err.println("ERROR writing results:");
                e.printStackTrace();
            }
        }
        System.out.println("All tests completed successfully!");



    }
}

