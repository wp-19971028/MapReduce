package demo05;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * 输入: <偏移量,文本,排序对象,空></>
 */
public class SortMapperTask extends Mapper<LongWritable, Text,SortPojo, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取行数据
        String line = value.toString();
        if(StringUtils.isNotEmpty(line)){
            //2.切割数据
            String[] sortPojoArr = line.split("\t");
            //3.封装对象并发送数据
            SortPojo sortPojo = new SortPojo();
            sortPojo.setFirst(sortPojoArr[0]);
            sortPojo.setSecond(Integer.parseInt(sortPojoArr[1]));
            context.write(sortPojo, NullWritable.get());
        }
    }
}