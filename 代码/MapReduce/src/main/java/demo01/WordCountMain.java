package demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //1.基于 tool 调用 run 方法
        Configuration conf = new Configuration();
        //2.运行并返回一个返回码 ,会返回两个值，如果成功就是0 如果失败就非0
        int code = ToolRunner.run(conf, new WordCountMain(), args);
        //3.执行程序，退出程序
        System.exit(code);

    }

    @Override
    public int run(String[] args) throws Exception {
        // 1. 获取job对象
        Job job = Job.getInstance(super.getConf(), "wordcount");
        // 2. 读取数据输入组件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\wordcount.txt"));
        // 3. 设置maptask
        job.setMapperClass(WordCoutMapperTask.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 4. 设置shuffle 分区 排序  规约 分组
        // 5 . 设置reducetask
        job.setReducerClass(WordCountReduceTask.class);
        job.setOutputKeyClass(Text.class);
        // 6. 输出组件
        job.setOutputFormatClass(TextOutputFormat.class);
        // 输出目录不能存在
        TextOutputFormat.setOutputPath(job,new Path("E:\\output"));
        //7 提交任务
        boolean flag = job.waitForCompletion(true);
        return flag?0:1;
    }
}