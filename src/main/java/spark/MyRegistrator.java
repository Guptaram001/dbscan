package spark;

import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(Point.class);
        kryo.register(PartitionConfiguration.class);

        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.HashMap.class);

    }
}
