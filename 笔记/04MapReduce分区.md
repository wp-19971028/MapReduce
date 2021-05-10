## 分区概述

>在 MapReduce 中, 通过我们指定分区, 会将同一个分区的数据发送到同一个Reduce当中进行处理。例如: 为了数据的统计, 可以把一批类似的数据发送到同一个 Reduce 当中, 在同一个 Reduce 当中统计相同类型的数据, 就可以实现类似的数据分区和统计等

> 其实就是相同类型的数据, 有共性的数据, 送到一起去处理, 在Reduce过程中，可以根据实际需求（比如按某个维度进行归档，类似于数据库的分组），把Map完的数据Reduce到不同的文件中。分区的设置需要与ReduceTaskNum配合使用。比如想要得到5个分区的数据结果。那么就得设置5个ReduceTask。

- 彩票案例

> 需求：将文本文件中的彩票数据进行分区，小于等于15的分到一个区里， 大于15的分到另外一个区里，并最终将数据保存到两个文件中。

> 思路：怎么进行数据的分区？

- mapreduce 默认分区的方式是 hashPartiton

```java
(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;  #默认情况 numReduceTasks =1
```

- 如何创建自定义分区
  1. 创建一个类，继承 Partitioner<K,V>
  2. 重写  getPartition 方法
  3. 自定义分区规则，小于等于1 5是个规则0，大于15是个规则1
  4. 在入口函数在中将分区规则设置到驱动类

1. 实现分区业务逻辑

```java
package demo03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Mypartition extends Partitioner<IntWritable, Text>{

    /* 根据输入的值 key2 value2 生成分区号
     * @param lotteryResult 彩票结果
     * @param text 每条彩票记录
     * @param numPartitions 分区的个数
     * @return 分区号
     */
    @Override
    public int getPartition(IntWritable lotteryResult, Text text, int numPartitions) {
        /**
         * 根据彩票号进行分区，如果小于等于15进行分区标记为0 否则1
         */
        if (lotteryResult.get() <= 15)
            return 0;
        else
            return 1;
    }
}
```

- 实现 map 业务逻辑

```java
实现reduce 的业务逻辑package demo03;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
 * Desc 将当前的文本内容按行进行拆分，得到第6列数据并转换成数字。
 */

public class LotteryMapperTask extends Mapper<LongWritable, Text, IntWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();
        if (StringUtils.isNotEmpty(line)){
            // 2. 对每个数据切割获取开奖结果
            String lotteryResult = line.split("\t")[5];
            int code = Integer.parseInt(lotteryResult);
            // 3. 将数据写出
            context.write(new IntWritable(code),value);

        }
    }
}
```

- 实现reduce 的业务逻辑

```java
package demo03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LotteryReducerTask extends Reducer<IntWritable, Text,Text, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values){
            context.write(value,NullWritable.get());
        }
    }
}
```

- 实现LotteryMain的业务逻辑 



本地

```java
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
                "E:\\input\\partition.csv"));
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
        TextOutputFormat.setOutputPath(job,new Path("E:\\output"));
        //2.9 设置使用分区数，自定义2个分区
        job.setNumReduceTasks(2);
        //3 提交任务
        boolean flag = job.waitForCompletion(true);
        int i = flag ? 0 : 1;
        System.exit(i);
    }
}
```

实验资料在本的的input文件夹里 

实验结果在output文件夹里

远程测试

```java
package demo04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Desc 彩票的分区需求提交集群
 */
public class LotteryMain {
    public static void main(String[] args) throws Exception{
        //1创建job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Lottery ticket MR");
        //设置提交到yarn集群的类
        job.setJarByClass(TextInputFormat.class);
        //2.封装 八大步
        //2.1 设置输入类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(args[0]));
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
        TextOutputFormat.setOutputPath(job,new Path(args[1]));
        //2.9 设置使用分区数，自定义2个分区
        job.setNumReduceTasks(2);
        //3 提交任务
        boolean flag = job.waitForCompletion(true);
        int i = flag ? 0 : 1;
        System.exit(i);
    }
}
```

```sh
yarn jar original-MR-1.0-SNAPSHOT.jar demo04.partition.LotteryMain /input/partition/partition.csv /output/partition
```

