package demo05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Desc 实现主要的封装操作
 */
public class SortMain {
    public static void main(String[] args) throws Exception {
        //1.创建job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort MR");
        //2:指定job所在的jar包
        job.setJarByClass(SortMain.class);
        //2.设置 job 的八大步  天龙八部
        //2.1 设置输入类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\MapReduce\\MapReduce\\input3\\a.txt"));
        //2.2 设置mapper
        job.setMapperClass(SortMapperTask.class);
        job.setMapOutputKeyClass(SortPojo.class);
        job.setMapOutputValueClass(NullWritable.class);
        //2.3 设置shuffle 分区 排序 combine 分组

        //2.7 设置reducer
        job.setReducerClass(SortReduceTask.class);
        job.setOutputKeyClass(SortPojo.class);
        job.setOutputValueClass(NullWritable.class);
        //3. 文件输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\MapReduce\\MapReduce\\output3"));
        // 提交任务
        boolean flag = job.waitForCompletion(true);
        //退出
        System.exit(flag?0:1);
    }
}