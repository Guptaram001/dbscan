package spark;

public interface QueryMetrics {

    void incrementQueryCount();

    void addQueryTime(long nanos);
}