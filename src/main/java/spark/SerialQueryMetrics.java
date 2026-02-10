package spark;

public class SerialQueryMetrics implements QueryMetrics {

    public long queryCount = 0;
    public long queryTime = 0;

    @Override
    public void incrementQueryCount() {
        queryCount++;
    }

    @Override
    public void addQueryTime(long nanos) {
        queryTime += nanos;
    }
}

