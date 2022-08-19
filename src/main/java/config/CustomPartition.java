package config;

import org.apache.spark.Partitioner;
import scala.Tuple3;
import scala.Tuple4;

public class CustomPartition extends Partitioner {

    private final int numParts;
    public CustomPartition(int i) {
        numParts = i;
    }
    @Override
    public int numPartitions() {
        return numParts;
    }

    @Override
    public int getPartition(Object key) {
        if (Integer.class.isAssignableFrom(key.getClass())){
            return (int) key;
        }
        if (Tuple3.class.isAssignableFrom(key.getClass())){
            Object cell = ((Tuple3<?, ?, ?>) key)._3();
            return (int) cell;
        }
        if (Tuple4.class.isAssignableFrom(key.getClass())){
            Object cell = ((Tuple4<?, ?, ?, ?>) key)._3();
            return (int) cell;
        }
        return -1;
    }


}
