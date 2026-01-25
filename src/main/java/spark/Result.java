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

    // Spark system metrics
    public long shuffleReadMBytes;
    public long shuffleWriteMBytes;
    public long memorySpilledBytes;
    public long diskSpilledBytes;

    public double neighborhoodTimeSec;  // T1
    public double ioTimeSec;             // T2
    public double totalTimeSec;          // Ts
    public double neighborhoodPercent;     // T1 / Ts
    public long dataScale;               // number of points


    @Override
    public String toString(){
        return "Result:"+eps+","+minPts+","+cellFactor+","+bufferFactor+","+mergeStrategy+","+runtimeMs+","+totalPoints+","
                +ghostPoints+","+neighborQueryCount+","+shuffleReadMBytes+","+shuffleWriteMBytes
                +","+memorySpilledBytes+","+diskSpilledBytes+","+neighborhoodPercent+","+dataScale+","+ioTimeSec+","+totalTimeSec+","+neighborhoodTimeSec;
    }

    public String printHeader(){
        return "eps,minPts,cellFactor,bufferFactor,mergeStrategy,runtimeMs,totalPoints,ghostPoints,neighborQueryCount," +
                "shuffleReadMBytes,shuffleWriteMBytes,memorySpilledBytes,diskSpilledBytes,"+
                "neighborhoodPercent,dataScale,ioTimeSec,totalTimeSec,neighborhoodTimeSec";
    }
}
