package spark;

import java.io.Serializable;

public class Result implements Serializable {
    public float eps;
    public int minPts;
    public float cellFactor;
    public float bufferFactor;
    public String mergeStrategy;
    public long runtimeMs;

    // ghost metrics
    public long totalPoints;
    public long ghostPoints;

    // DBSCAN metrics
    public long neighborQueryCount;
    public float neighborQueryTimeNs;

    // Spark system metrics
    public long shuffleReadBytes;
    public long shuffleWriteBytes;
    public long memorySpilledBytes;
    public long diskSpilledBytes;

    @Override
    public String toString(){
        return eps+","+minPts+","+cellFactor+","+bufferFactor+","+mergeStrategy+","+runtimeMs+","+totalPoints+","
                +ghostPoints+","+neighborQueryCount+","+neighborQueryTimeNs+","+shuffleReadBytes+","+shuffleWriteBytes
                +","+memorySpilledBytes+","+diskSpilledBytes;
    }

    public String printHeader(){
        return "eps,minPts,cellFactor,bufferFactor,mergeStrategy,runtimeMs,totalPoints,ghostPoints,neighborQueryCount," +
                "neighborQueryTimeNs,shuffleReadBytes,shuffleWriteBytes,memorySpilledBytes,diskSpilledBytes";
    }
}
