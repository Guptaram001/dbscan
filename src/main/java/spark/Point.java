package spark;

import java.io.Serializable;

public class Point implements Serializable {
    long id;
    float latitude;
    float longitude;
    int clusterId;
    boolean isLocalRegion;
    boolean isCorePoint;
    int cellId;

    Point(long id,float latitude, float longitude, int clusterId) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.clusterId = clusterId;
    }

//    @Override
//    public String toString() {
//        return "(id:" + id +  ", Lat:" + latitude + ", Long:" + longitude + ", CId:" + clusterId + ", IsLocal:" + isLocalRegion + ", IsCore:"+isCorePoint+", HomeCell: "+cellId + " )" ;
//    }

    @Override
    public String toString() {
        return "(" + id +  ", " + latitude + ", " + longitude + ", " + clusterId + ", " + isLocalRegion + ", "+isCorePoint+", "+cellId + " )" ;
    }
}
