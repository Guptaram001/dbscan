package spark;
import java.io.Serializable;

public class GridKey implements Serializable {
    public final int gx;
    public final int gy;

    public GridKey(int gx, int gy) {
        this.gx = gx;
        this.gy = gy;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GridKey)) return false;
        GridKey other = (GridKey) o;
        return gx == other.gx && gy == other.gy;
    }

    @Override
    public int hashCode() {
        return 31 * gx + gy;
    }
}
