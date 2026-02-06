package spark;

import java.io.Serializable;

public class PartitionConfiguration implements Serializable {

        final float minLatitude, maxLatitude, minLongitude, maxLongitude;
        public final float cellSize;
        public final float buffer;
        public final int numCellsX, numCellsY;

        public PartitionConfiguration(float minX, float maxX, float minY, float maxY, float eps,float cellSize,float buffer) {
            this.minLatitude = minX;
            this.maxLatitude = maxX;
            this.minLongitude = minY;
            this.maxLongitude = maxY;
            this.cellSize = cellSize * eps;
            this.buffer = buffer * eps;
            this.numCellsX = (int) Math.ceil((maxX - minX) / cellSize) + 1;
            this.numCellsY = (int) Math.ceil((maxY - minY) / cellSize) + 1;
        }

}
