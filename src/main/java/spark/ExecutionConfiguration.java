package spark;

public class ExecutionConfiguration {
    public final float eps;
    public final int minPts;
    public final float cellFactor;
    public final String mergeStrategy;
    public final String inputPath;
    public final float bufferFactor;
    public final boolean DEBUG;

    public ExecutionConfiguration(float eps, int minPts, float cellFactor, float bufferFactor,String mergeStrategy, boolean DEBUG,String inputPath) {
        this.eps = eps;
        this.minPts = minPts;
        this.cellFactor = cellFactor;
        this.mergeStrategy = mergeStrategy;
        this.inputPath = inputPath;
        this.bufferFactor = bufferFactor;
        this.DEBUG = DEBUG;
    }

    public  String formId() {
        return "eps: " + eps + ", minPts: " + minPts + ", cellFactor: " + cellFactor + ",bufferFactor: " + bufferFactor+", mergeStrategy: " + mergeStrategy;
    }

}
