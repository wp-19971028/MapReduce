package demo11;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Friend2MapperTask extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 判空
        if(StringUtils.isNotEmpty(line)){
            // 切割
            String[] splits = line.split("\t");
            String[] friends = splits[1].split("-");
            // 从小到大排序
            Arrays.sort(friends);
            //遍历 A  B-C-F-G-H-I-K-O
            for(int i=0;i<friends.length-1;i++){
                for(int j=i+1;j<friends.length;j++){
                    String k2 = friends[i] + "-" + friends[j];
                    //写出去
                    context.write(new Text(k2),new Text(splits[0]));
                }
            }
        }
    }
}
