package demo03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Desc 彩票的分区需求本地测试
 */
public class LotteryMain {
    public static void main(String[] args) throws Exception{
        //1创建job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Lottery ticket MR");
        //2.封装 八大步
        //2.1 设置输入类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(
                "E:\\MapReduce\\MapReduce\\input\\partition.csv"));
        //2.2 设置自定义map类和相关参数
        job.setMapperClass(LotteryMapperTask.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        //2.3 设置shuffle中的分区
        job.setPartitionerClass(Mypartition.class);
        //2.4 排序、combine、分组
        //2.7 设置reduce类和相关参数
        job.setReducerClass(LotteryReducerTask.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //2.8 设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\MapReduce\\MapReduce\\output"));
        //2.9 设置使用分区数，自定义2个分区
        job.setNumReduceTasks(2);
        //3 提交任务
        boolean flag = job.waitForCompletion(true);
        int i = flag ? 0 : 1;
        System.exit(i);
    }
}