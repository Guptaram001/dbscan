package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.FileWriter;


import java.util.*;

public class SerialDBSCAN {
    public static Result executeDBSCAN(JavaSparkContext sc, ExecutionConfiguration executionConfiguration, SparkMetricListener sparkMetricListener) throws Exception {

        Result result = new Result();
        result.eps=executionConfiguration.eps;
        result.minPts=executionConfiguration.minPts;
        result.cellFactor=executionConfiguration.cellFactor;
        result.bufferFactor=executionConfiguration.bufferFactor;
        result.mergeStrategy=executionConfiguration.mergeStrategy;
        float eps2=result.eps*result.eps;
        FileWriter out = new FileWriter("results/results.csv");

        String inputPath = executionConfiguration.inputPath;

        LongAccumulator neighborQueryCount = sc.sc().longAccumulator("neighborQueryCount");
        LongAccumulator neighborQueryTimeNs = sc.sc().longAccumulator("neighborQueryTimeNs");
        long startTime = System.currentTimeMillis();

        //Reads from the file and associates each with a long index as Point(index, lat, long, clusterid =0)
        long readStart = System.currentTimeMillis();
        JavaRDD<Point> points = sc.textFile(inputPath)
                .zipWithIndex()
                .filter(t -> !t._1.trim().isEmpty())
                .map(t -> {
                    String[] parts = t._1.trim().split(",");
                    float x = Float.parseFloat(parts[0]);
                    float y = Float.parseFloat(parts[1]);
                    return new Point(t._2, x, y, 0);
                });
        List<Point> pointList = points.collect();
        long readEnd = System.currentTimeMillis();

        System.out.println("Points running");
        long totalPoints = pointList.size();

        Utils.localDBSCAN(pointList, eps2, result.minPts, neighborQueryCount, neighborQueryTimeNs);
        long writeStart = System.currentTimeMillis();
        for(Point point : pointList) {
            out.write(point.toString()+"\n");
            System.out.println("Points:"+point.toString());
        }
        long writeEnd = System.currentTimeMillis();

        System.out.println("Points completed");
        out.flush();
        out.close();

        long endTime = System.currentTimeMillis();
        result.runtimeMs = endTime - startTime;
        result.neighborQueryCount = neighborQueryCount.value();
        result.shuffleReadMBytes =sparkMetricListener.shuffleRead;
        result.shuffleWriteMBytes=sparkMetricListener.shuffleWrite;
        result.diskSpilledBytes=sparkMetricListener.diskSpilled;
        result.memorySpilledBytes=sparkMetricListener.memorySpilled;

        double T1Sec = neighborQueryTimeNs.value() / 1e9;
        double readSec = (readEnd - readStart) / 1000.0;
        double writeSec = (writeEnd - writeStart) / 1000.0;
        double T2Sec = readSec + writeSec;
        double totalSec = (endTime - startTime) / 1000.0;
        double ratio = (T1Sec / totalSec) * 100.0;

        result.neighborhoodTimeSec = T1Sec;
        result.ioTimeSec = T2Sec;
        result.totalTimeSec = totalSec;
        result.neighborhoodPercent = ratio;
        result.dataScale = totalPoints;
        return  result;
    }

}



