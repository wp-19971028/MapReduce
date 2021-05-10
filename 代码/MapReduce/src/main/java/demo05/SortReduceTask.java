package demo05;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SortReduceTask extends Reducer<SortPojo, NullWritable,SortPojo, NullWritable> {
    @Override
    protected void reduce(SortPojo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}