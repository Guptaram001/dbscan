package spark;

import java.io.Serializable;

public class PartitionConfiguration implements Serializable {


    final float[] minCoords;  // minimum coordinate for each dimension
    final float[] maxCoords;  // maximum coordinate for each dimension
    final float cellSize;
    final float buffer;
    final int[] numCellsPerDim;  // number of cells in each dimension
    final int dimensions;
    final long totalCells;  // total number of cells (product of numCellsPerDim)

    // Constructor for n-dimensional data
    PartitionConfiguration(float[] minCoords, float[] maxCoords, float eps,float cellFactor,float bufferFactor) {
        this.dimensions = minCoords.length;
        this.minCoords = minCoords.clone();
        this.maxCoords = maxCoords.clone();
        this.cellSize = cellFactor * eps;
        this.buffer = bufferFactor * eps;

        this.numCellsPerDim = new int[dimensions];
        long total = 1;
        for (int i = 0; i < dimensions; i++) {
            this.numCellsPerDim[i] = (int) Math.ceil((maxCoords[i] - minCoords[i]) / cellSize) + 1;
            total *= this.numCellsPerDim[i];
        }
        this.totalCells = total;
    }

    // Constructor for backward compatibility (2D)
    PartitionConfiguration(float minX, float maxX, float minY, float maxY, float eps,float cellFactor,float bufferFactor) {
        this(new float[]{minX, minY}, new float[]{maxX, maxY}, eps,cellFactor,bufferFactor);
    }

    // Convert n-dimensional cell coordinates to 1D cell ID
    // Uses row-major order: cellId = sum(cellCoords[i] * product(numCellsPerDim[j] for j < i))
    public int cellCoordsToId(int[] cellCoords) {
        int cellId = 0;
        int multiplier = 1;
        for (int i = dimensions - 1; i >= 0; i--) {
            cellId += cellCoords[i] * multiplier;
            multiplier *= numCellsPerDim[i];
        }
        return cellId;
    }

    // Convert 1D cell ID to n-dimensional cell coordinates
    public int[] cellIdToCoords(int cellId) {
        int[] coords = new int[dimensions];
        int remaining = cellId;
        for (int i = dimensions - 1; i >= 0; i--) {
            coords[i] = remaining % numCellsPerDim[i];
            remaining /= numCellsPerDim[i];
        }
        return coords;
    }

    // Get cell coordinates for a point
    public int[] getCellCoords(float[] pointCoords) {
        int[] cellCoords = new int[dimensions];
        for (int i = 0; i < dimensions; i++) {
            int coord = (int) Math.floor((pointCoords[i] - minCoords[i]) / cellSize);
            cellCoords[i] = Math.max(0, Math.min(coord, numCellsPerDim[i] - 1));
        }
        return cellCoords;
    }

    // Get minimum coordinates of a cell
    public float[] getCellMinCoords(int[] cellCoords) {
        float[] minCoords = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            minCoords[i] = this.minCoords[i] + cellCoords[i] * cellSize;
        }
        return minCoords;
    }

//        public final float minLatitude, maxLatitude, minLongitude, maxLongitude;
//        public final float cellSize;
//        public final float buffer;
//        public final int numCellsX, numCellsY;
//
//        public PartitionConfiguration(float minX, float maxX, float minY, float maxY, float eps,float cellFactor,float bufferFactor) {
//            this.minLatitude = minX;
//            this.maxLatitude = maxX;
//            this.minLongitude = minY;
//            this.maxLongitude = maxY;
//            this.cellSize = cellFactor * eps;
//            this.buffer = bufferFactor * eps;
//            this.numCellsX = (int) Math.ceil((maxX - minX) / this.cellSize) ;
//            this.numCellsY = (int) Math.ceil((maxY - minY) / this.cellSize) ;
//        }
}
