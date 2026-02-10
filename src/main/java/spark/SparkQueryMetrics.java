package spark;

import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

public class SparkQueryMetrics   implements QueryMetrics,Serializable  {

    private final LongAccumulator queryCount;
    private final LongAccumulator queryTime;

    public SparkQueryMetrics(LongAccumulator queryCount, LongAccumulator queryTime) {
        this.queryCount = queryCount;
        this.queryTime = queryTime;
    }

    @Override
    public void incrementQueryCount() {
        queryCount.add(1);
    }

    @Override
    public void addQueryTime(long nanos) {
        queryTime.add(nanos);
    }
}
