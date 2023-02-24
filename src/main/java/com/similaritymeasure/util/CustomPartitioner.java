package com.similaritymeasure.util;
import lombok.AllArgsConstructor;
import org.apache.spark.Partitioner;
import scala.Tuple2;

@AllArgsConstructor
public class CustomPartitioner extends Partitioner {

    private int numPartitions;

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        if (Integer.class.isAssignableFrom(key.getClass())){
            return (int) key;
        }
        if (Tuple2.class.isAssignableFrom(key.getClass())) {
            if (((Tuple2<?, ?>) key)._1() instanceof Tuple2){
                return (int) ((Tuple2) ((Tuple2<?, ?>) key)._1())._2;
            }
            Object cell = ((Tuple2<?, ?>) key)._2();
            return (int) cell;
        }

        return -1;
    }

}