package spark;

public class GeoLocation {
    private double latitude;
    private double longitude;
    private  String type;
    public GeoLocation(String type,double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.type = type;

    }

    public double getLatitude() {
        return latitude;
    }
    public double getLongitude() {
        return longitude;
    }
    public String getType() {
        return type;
    }

}
