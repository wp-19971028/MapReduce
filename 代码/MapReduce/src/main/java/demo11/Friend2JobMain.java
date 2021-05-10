package demo11;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Friend2JobMain {
    public static void main(String[] args) throws Exception {
        //1.创建job对象
        Job job = Job.getInstance(new Configuration(), "Friend2JobMR");
        //2.组装八大步
        //2.1输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("E:\\MapReduce\\MapReduce\\共同好友\\output"));
        //2.2设置mapper
        job.setMapperClass(Friend2MapperTask.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //2.3 shuffle 分区 排序 规约 分组
        //2.7 设置reduce
        job.setReducerClass(Friend2ReducerTask.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //2.8 输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("E:\\MapReduce\\MapReduce\\共同好友\\output2"));
        //3 提交任务
        boolean flag = job.waitForCompletion(true);
        //4 退出执行
        System.exit(flag?0:1);
    }
}
