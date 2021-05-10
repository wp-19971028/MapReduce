package demo06;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CombinerMapperTask extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String bookName = value.toString();
        // 判断是否为空
        if (StringUtils.isNotEmpty(bookName)){
            if (bookName.contains("入门")){
                context.write(new Text("计算机"),new IntWritable(1));
            }else if(bookName.contains("史记")|| bookName.contains("论清王朝的腐败")){
                context.write(new Text("历史"),new IntWritable(1));
            }else {
                context.write(new Text("武林秘籍"),new IntWritable(1));
            }
        }
    }
}
