package demo07;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapperTask extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取每一行数据
        String line = value.toString();
        // 判断数据不为空
        if(StringUtils.isNotEmpty(line)){
            //2.1对数据切割
            String[] splits = line.split("\t");
            //2.2数据进行封装
            OrderBean orderBean = new OrderBean();
            orderBean.setOrderid(splits[0]);
            orderBean.setPid(splits[1]);
            orderBean.setPrice(Double.parseDouble(splits[2]));
            //将数据写出去
            context.write(orderBean,NullWritable.get());
        }
    }
}
