package spark;

import java.io.Serializable;

public class Result implements Serializable {
    public String runId;
    public float eps;
    public int minPts;
    public float cellFactor;
    public float bufferFactor;
    public String mergeStrategy;

    // ghost metrics
    public long totalPoints;
    public long ghostPoints;

    // DBSCAN metrics
    public long neighborQueryCount;

    // Spark system metrics
    public float shuffleReadMBytes;
    public float shuffleWriteMBytes;
    public float memorySpilledBytes;
    public float diskSpilledBytes;

    public double neighborhoodTimeSec;  // T1
    public double ioTimeSec;             // T2
    public double totalTimeSec;          // Ts
    public double neighborhoodPercent;     // T1 / Ts
    public long dataScale;
    public int numClusters;
    public int noisePoints;


    @Override
    public String toString(){
        return runId+","+eps+","+minPts+","+cellFactor+","+bufferFactor+","+mergeStrategy+","+totalPoints+","
                +ghostPoints+","+neighborQueryCount+","+shuffleReadMBytes+","+shuffleWriteMBytes
                +","+memorySpilledBytes+","+diskSpilledBytes+","+neighborhoodPercent+","+dataScale+","+ioTimeSec+","+totalTimeSec+","+neighborhoodTimeSec+","+numClusters+","+noisePoints;
    }

    public String printHeader(){
        return "runId, eps,minPts,cellFactor,bufferFactor,mergeStrategy,totalPoints,ghostPoints,neighborQueryCount," +
                "shuffleReadMBytes,shuffleWriteMBytes,memorySpilledBytes,diskSpilledBytes,"+
                "neighborhoodPercent,dataScale,ioTimeSec,totalTimeSec,neighborhoodTimeSec,numClusters,noisePoints";
    }
}
