package demo08;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduceTask  extends Reducer<Text, FlowBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //用于接收总的上传流量
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upTotalFlow = 0;
        Integer downTotalFlow = 0;
        //变量flowBean
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upTotalFlow += value.getUpCountFlow();
            downTotalFlow += value.getDownCountFlow();
        }
        //将值写出去
        String k3 = key.toString() + "\t" + upFlow + "\t" + downFlow + "\t" + upTotalFlow + "\t" + downTotalFlow;
        context.write(new Text(k3), NullWritable.get());
    }
}
