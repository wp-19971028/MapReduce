### **概念**

> 每一个 map 都可能会产生大量的本地输出，Combiner 的作用就是对 map 端的输出先做一次合并，以减少在 map 和 reduce 节点之间的数据传输量，以提高网络IO 性能，是 MapReduce 的一种优化手段之一

- combiner 是 MR 程序中 Mapper 和 Reducer 之外的一种组件

- combiner 组件的父类就是 Reducer所以极度相似

- combiner 和 reducer 的区别在于运行的位置
  - Combiner 是在每一个 maptask 所在的节点运行
  -  Reducer 是接收全局所有 Mapper 的输出结果

- combiner 的意义就是对每一个 maptask 的输出进行局部汇总，以减小网络传输量

  - 原理:对map 阶段局部汇总之后会减少 map 阶段输出的结果的数量，减少 reduce 端拉取数据的数据量，提升整体MR的运行效率。

- 案例：
  - 需求:  有 三个书架 ,每个书架上都有5本书, 要求 统计出 每种分类的下有几本书?  计算机  武林秘籍  历史

  ~~~
    1号书架                 2号书架                         3号书架
  <<java入门宝典>>      <<Python入门宝典>>            <<spark入门宝典>>
  <<UI入门宝典>>		  <<乾坤大挪移>>                <<hive入门宝典>>
  <<天龙八部>>		  <<凌波微步>>                  <<葵花点穴手>>
  <<史记>>		       <<PHP入门宝典>>               <<铁砂掌>>
  <<葵花宝典>>          <<hadoop入门宝典>>            <<论清王朝的腐败>>
  ~~~

  - 思路:
    1. 首先要创建 mapTask reduceTask
    2. 重写Reducer提供的reduce方法，实现局部聚合逻辑
    3. 在MR的驱动中，main函数中将 combine 的类添加进去。

map

```java
package demo06;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CombinerMapperTask extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String bookName = value.toString();
        // 判断是否为空
        if (StringUtils.isNotEmpty(bookName)){
            if (bookName.contains("入门")){
                context.write(new Text("计算机"),new IntWritable(1));
            }else if(bookName.contains("史记")|| bookName.contains("论清王朝的腐败")){
                context.write(new Text("历史"),new IntWritable(1));
            }else {
                context.write(new Text("武林秘籍"),new IntWritable(1));
            }
        }
    }
}
```

reduce

```java
package demo06;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerReducerTask extends Reducer <Text, IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value:values){
            count += value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
```

combin

```java
package demo06;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerTask extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values){
            count += value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
```

main

```java
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
```

- 使用combiner 和不使用 combiner 的reduce端的数据 input 的区别，使用combiner 明显比不使用 combiner的读取数量要小。

![image-20200823103648128](./assets\image-20200823103648128.png)