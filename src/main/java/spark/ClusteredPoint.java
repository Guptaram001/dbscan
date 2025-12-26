package spark;

import java.io.Serializable;

public class ClusteredPoint implements Serializable {
    Point point;
    int localClusterId;
    boolean isCorePoint;
    boolean isLocalRegion;

    ClusteredPoint(double id,double x,double y,int clusterId, boolean isCorePoint, boolean isLocalRegion) {
        this.point=new Point(id,x,y,clusterId);
        this.localClusterId = clusterId;
        this.isCorePoint = isCorePoint;
        this.isLocalRegion = isLocalRegion;
    }

    @Override
    public String toString() {
        return "(" +point+", "+ localClusterId + ", " + isCorePoint + ", " + isLocalRegion + ")";
    }
}


