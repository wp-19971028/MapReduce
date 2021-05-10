package demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduceTask extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1.定义一个计数器
        int count = 0;
        // 2. 遍历所有值
        for (IntWritable v2 : values){
            count += v2.get();
        }
        // 3. 对对象累加值进行操作
        context.write(key,new IntWritable(count));
    }
}

















