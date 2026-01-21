package spark;

import java.io.Serializable;

public class PartitionConfiguration implements Serializable {

        final double minLatitude, maxLatitude, minLongitude, maxLongitude;
        final double cellSize;
        final double buffer;
        final int numCellsX, numCellsY;

        PartitionConfiguration(double minX, double maxX, double minY, double maxY, double eps) {
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
