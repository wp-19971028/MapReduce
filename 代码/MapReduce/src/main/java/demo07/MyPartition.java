package demo07;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition  extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        String orderid = orderBean.getOrderid();
        return (orderid.hashCode() & Integer.MAX_VALUE) % i;
    }
}
