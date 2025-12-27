package spark;

import java.io.Serializable;

public class Point implements Serializable {
    double id;
    double latitude;
    double longitude;
    int clusterId;
    boolean isLocalRegion;
    boolean isCorePoint;
    int cellId;
    int globalClusterId;



    Point(double id,double latitude, double longitude, int clusterId) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.clusterId = clusterId;
    }

    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public int getClusterId() { return clusterId; }
    public boolean isLocalRegion() { return isLocalRegion; }
    public boolean isCorePoint() { return isCorePoint; }
    public int getCellId() { return cellId; }
    public void setCellId(int cellId) { this.cellId = cellId; }
    public int getglobalClusterId() { return globalClusterId; }

    @Override
    public String toString() {
        return "(" + id +  ", " + latitude + ", " + longitude + ", " + clusterId + ", " + isLocalRegion + ", "+isCorePoint+" ,"+cellId + " )" ;
    }


}
