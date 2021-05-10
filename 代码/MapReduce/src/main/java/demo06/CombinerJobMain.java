package demo06;


import com.google.protobuf.TextFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CombinerJobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 创建job对象
        Job job = Job.getInstance(new Configuration(), "combinerJobMR");
        job.setJarByClass(CombinerJobMain.class);
        // 2. 设置MapReduce 的八大步
        // 2.1 读取数据
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\MapReduce\\MapReduce\\inputdemo06\\combiner.txt"));
        // 2.2 设置map
        job.setMapperClass(CombinerMapperTask.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //2.3 设置shuffle : 分区 排序 归并 combine 分组
        job.setCombinerClass(CombinerReducerTask.class);
        // 2.7 设置 reduce
        job.setReducerClass(CombinerReducerTask.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //2.8 输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\MapReduce\\MapReduce\\outputdemo06"));
        // 3. 提交任务
        boolean flag = job.waitForCompletion(true);
        // 4. 退出程序
        System.exit(flag?0:1);
    }



}
