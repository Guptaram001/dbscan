package spark;

import java.io.Serializable;

public class Point implements Serializable {
    long id;
    double[] coordinates;  // n-dimensional coordinates
    int dimensions;  // number of dimensions
    int clusterId;
    boolean isLocalRegion;
    boolean isCorePoint;
    int cellId;
    int globalClusterId;

    Point(long id, double[] coordinates, int clusterId) {
        this.id = id;
        this.coordinates = coordinates.clone();
        this.dimensions = coordinates.length;
        this.clusterId = clusterId;
    }

    // Constructor for compatibility (2D)
//    Point(long id, float x, float y, int clusterId) {
//        this.id = id;
//        this.coordinates = new float[]{x, y};
//        this.dimensions = 2;
//        this.clusterId = clusterId;
//    }

    public double[] getCoordinates() { return coordinates.clone(); }
    public int getDimensions() { return dimensions; }
    public double getCoordinate(int dim) { return coordinates[dim]; }
    public int getClusterId() { return clusterId; }
    public boolean isLocalRegion() { return isLocalRegion; }
    public boolean isCorePoint() { return isCorePoint; }
    public int getCellId() { return cellId; }
    public void setCellId(int cellId) { this.cellId = cellId; }
    public int getglobalClusterId() { return globalClusterId; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(id).append(", [");
        for (int i = 0; i < coordinates.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(coordinates[i]);
        }
        sb.append("], ").append(clusterId).append(", ").append(isLocalRegion)
                .append(", ").append(isCorePoint).append(", ").append(cellId).append(")");
        return sb.toString();
    }


//    public long id;
//    public float latitude;
//    public float longitude;
//    public int clusterId;
//    public boolean isLocalRegion;
//    public boolean isCorePoint;
//    public int cellId;
//
//    public Point(long id,float latitude, float longitude, int clusterId) {
//        this.id = id;
//        this.latitude = latitude;
//        this.longitude = longitude;
//        this.clusterId = clusterId;
//    }
//
////    @Override
////    public String toString() {
////        return "(id:" + id +  ", Lat:" + latitude + ", Long:" + longitude + ", CId:" + clusterId + ", IsLocal:" + isLocalRegion + ", IsCore:"+isCorePoint+", HomeCell: "+cellId + " )" ;
////    }
//
//    @Override
//    public String toString() {
//        return "(" + id +  ", " + latitude + ", " + longitude + ", " + clusterId + ", " + isLocalRegion + ", "+isCorePoint+", "+cellId + " )" ;
//    }
}
