package spark;

import java.io.Serializable;

public class PartitionConfiguration implements Serializable {

        final float minLatitude, maxLatitude, minLongitude, maxLongitude;
        final float cellSize;
        final float buffer;
        final int numCellsX, numCellsY;

        PartitionConfiguration(float minX, float maxX, float minY, float maxY, float eps) {
            this.minLatitude = minX;
            this.maxLatitude = maxX;
            this.minLongitude = minY;
            this.maxLongitude = maxY;
            this.cellSize = 3 * eps;
            this.buffer = 1 * eps;
            this.numCellsX = (int) Math.ceil((maxX - minX) / cellSize) + 1;
            this.numCellsY = (int) Math.ceil((maxY - minY) / cellSize) + 1;
        }

}
