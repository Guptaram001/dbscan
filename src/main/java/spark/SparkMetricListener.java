package spark;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

public class SparkMetricListener extends SparkListener {
    public long shuffleRead = 0;
    public long shuffleWrite = 0;
    public long memorySpilled = 0;
    public long diskSpilled = 0;
    public int stages = 0;
    public int tasks = 0;

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stage) {
        stages++;

        TaskMetrics m = stage.stageInfo().taskMetrics();
        if (m != null) {
            shuffleRead += m.shuffleReadMetrics().totalBytesRead();
            shuffleWrite += m.shuffleWriteMetrics().bytesWritten();
            memorySpilled += m.memoryBytesSpilled();
            diskSpilled += m.diskBytesSpilled();
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        tasks++;
    }

    @Override
    public String toString() {
        return "Obtained Spark Metrics - Stages: " + stages +  ", Tasks: " + tasks + ", Shuffle Read (MB): " + shuffleRead/ 1e6 + ", Shuffle Write (MB): " + shuffleWrite/ 1e6 + ", Memory Spilled (MB): " + memorySpilled/ 1e6 + ", Disk Spilled (MB):"+diskSpilled/ 1e6 ;
    }
}
