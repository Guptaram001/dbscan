package spark;

import java.io.Serializable;

public class Point implements Serializable {
    double latitude;
    double longitude;
    int clusterId;
    boolean isLocalRegion;
    boolean isCorePoint;
    int cellId;



    Point(double latitude, double longitude, int clusterId) {
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

    @Override
    public String toString() {
        return "(" + latitude + ", " + longitude + ", " + clusterId + ", " + isLocalRegion + ", "+isCorePoint+" ,"+cellId + " )" ;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Point)) return false;
        Point p = (Point) o;
        return Double.compare(p.latitude, latitude) == 0 &&
                Double.compare(p.longitude, longitude) == 0;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(latitude, longitude);
    }

}
