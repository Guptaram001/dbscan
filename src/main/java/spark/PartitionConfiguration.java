package spark;

import java.io.Serializable;

public class PartitionConfiguration implements Serializable {

        public final float minLatitude, maxLatitude, minLongitude, maxLongitude;
        public final float cellSize;
        public final float buffer;
        public final int numCellsX, numCellsY;

        public PartitionConfiguration(float minX, float maxX, float minY, float maxY, float eps,float cellFactor,float bufferFactor) {
            this.minLatitude = minX;
            this.maxLatitude = maxX;
            this.minLongitude = minY;
            this.maxLongitude = maxY;
            this.cellSize = cellFactor * eps;
            this.buffer = bufferFactor * eps;
            this.numCellsX = (int) Math.ceil((maxX - minX) / this.cellSize) ;
            this.numCellsY = (int) Math.ceil((maxY - minY) / this.cellSize) ;
        }
}
