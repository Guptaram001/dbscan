package spark;


import java.io.BufferedReader;
import java.io.FileWriter;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class SerialDBSCAN {
    public static Result executeDBSCAN(ExecutionConfiguration executionConfiguration, String runId) throws Exception {

        Result result = new Result();
        result.runId=runId;
        result.eps=executionConfiguration.eps;
        result.minPts=executionConfiguration.minPts;
        result.cellFactor=0;
        result.bufferFactor=0;
        result.mergeStrategy=executionConfiguration.mergeStrategy;
        float eps2=result.eps*result.eps;
        //FileWriter out = new FileWriter("results/results.csv");
        Files.createDirectories(Paths.get("results/SerialDBSCAN_"+runId));
        FileWriter out = new FileWriter("results/SerialDBSCAN_"+runId+"/part-00000");

        String inputPath = executionConfiguration.inputPath;
        System.out.println("SerialDBSCAN started");
        System.out.println("Reading input file: " + inputPath);

        long startTime = System.currentTimeMillis();
        SerialQueryMetrics metrics = new SerialQueryMetrics();

        //Reads from the file and associates each with a long index as Point(index, lat, long, clusterid =0)
        long readStart = System.currentTimeMillis();

        List<Point> pointList = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(inputPath))) {
            String line;
            long id = 0;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split(",");
                double[] coords = new double[parts.length];
                for (int i = 0; i < parts.length; i++) {
                    coords[i] = Float.parseFloat(parts[i]);
                }
                pointList.add(new Point(id++, coords, 0));
            }
        }

        long readEnd = System.currentTimeMillis();
        long totalPoints = pointList.size();

        Utils.localDBSCAN(pointList, eps2, result.minPts,metrics);

        long writeStart = System.currentTimeMillis();
        for(Point point : pointList) {
            out.write(point.toString()+"\n");
        }
        long writeEnd = System.currentTimeMillis();

        out.flush();
        out.close();
        long endTime = System.currentTimeMillis();

        Set<Integer> clusters = new HashSet<>();
        int noise = 0;
        for (Point p : pointList) {
            if (p.clusterId > 0) {
                clusters.add(p.clusterId);
            } else if (p.clusterId == -1) {
                noise++;
            }
        }

        result.numClusters = clusters.size();
        result.noisePoints = noise;
        result.neighborQueryCount = metrics.queryCount;
        result.shuffleReadMBytes =0;
        result.shuffleWriteMBytes=0;
        result.diskSpilledBytes=0;
        result.memorySpilledBytes=0;

        double T1Sec = metrics.queryTime / 1e9;
        double readSec = (readEnd - readStart) / 1000.0;
        double writeSec = (writeEnd - writeStart) / 1000.0;
        double T2Sec = readSec + writeSec;
        double totalSec = (endTime - startTime) / 1000.0;
        double ratio = (T1Sec / totalSec) * 100.0;

        result.neighborhoodTimeSec = T1Sec;
        result.ioTimeSec = T2Sec;
        result.totalTimeSec = totalSec;
        result.neighborhoodPercent = ratio;
        return  result;
    }

}



