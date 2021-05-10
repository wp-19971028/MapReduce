package demo01;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author wp
 * Date 2021/5/4 13:21
 * Desc 1.实现读取文件 2.将数据进行map映射处理
 */
public class WordCoutMapperTask extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("偏移量"+key.get());
        // 1. 读取每一行数据
        String line = value.toString();
        // 2. 判断行数据是否为空
        if(StringUtils.isNotEmpty(line)){
            // 3. 对数据进行切割
            String[] words = line.split(",");
            // 4. 对每个单词赋值为1
            for(String word : words){
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
}
