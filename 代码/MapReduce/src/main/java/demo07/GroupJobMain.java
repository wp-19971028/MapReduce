package demo07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupJobMain {
    public static void main(String[] args) throws Exception{
        //1创建job对象
        Job job = Job.getInstance(new Configuration(), "GroupJobMR");
        //2 实现mr 八大步
        //2.1 读取数据
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\MapReduce\\MapReduce\\inputdemo07\\orders.txt"));
        //2.2 封装 mapper
        job.setMapperClass(GroupMapperTask.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //2.3 分区操作
        job.setPartitionerClass(MyPartition.class);
        //2.4 设置排序，规约
        //2.6 设置分组操作
        job.setGroupingComparatorClass(MyGroup.class);
        //2.7 设置reduce
        job.setReducerClass(GroupReduceTask.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //2.8 输出操作
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\MapReduce\\MapReduce\\outputdemo07"));
        //3.设置 2个reduce task
        job.setNumReduceTasks(2);
        //4 提交任务
        boolean flag = job.waitForCompletion(true);
        //5 退出
        System.exit(flag?0:1);
    }
}
