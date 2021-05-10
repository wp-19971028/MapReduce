package demo03;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
 * Desc 将当前的文本内容按行进行拆分，得到第6列数据并转换成数字。
 */

public class LotteryMapperTask extends Mapper<LongWritable, Text, IntWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();
        if (StringUtils.isNotEmpty(line)){
            // 2. 对每个数据切割获取开奖结果
            String lotteryResult = line.split("\t")[5];
            int code = Integer.parseInt(lotteryResult);
            // 3. 将数据写出
            context.write(new IntWritable(code),value);

        }
    }
}
