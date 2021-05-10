package demo08;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapperTask  extends Mapper<LongWritable, Text, Text/*手机号*/,FlowBean/*流量对象*/> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取数据
        String line = value.toString();
        //2.判断是否为空
        if(StringUtils.isNotEmpty(line)){
            String[] splits = line.split("\t");
            //截取出来四个字段 上传流量 下载流量 上传总流量 下载总流量
            String iphone = splits[1];
            String upFlow = splits[6];
            String downFlow = splits[7];
            String upTotalFlow = splits[8];
            String downTotalFlow = splits[9];
            FlowBean flowBean = new FlowBean(
                    Integer.parseInt(upFlow),
                    Integer.parseInt(downFlow),
                    Integer.parseInt(upTotalFlow),
                    Integer.parseInt(downTotalFlow)
            );
            //将数据写出去
            context.write(new Text(iphone),flowBean);
        }
    }
}
