package demo10;



import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Friend1MapperTask extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取每一行数据
        String line = value.toString();
        // 判断每一行是否存在
        if (StringUtils.isNotEmpty(line)){
            // 对数据进行切割
            String[] splits = line.split(":");
            String[] friends = splits[1].split(",");
            // 遍历好友
            for (String friend:friends){
                // 写出去
                context.write(new Text(friend),new Text(splits[0]));
            }
        }
    }
}
