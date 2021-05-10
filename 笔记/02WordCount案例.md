# WordCount（单词统计）的入门案例

## 分析这个案例怎么实现

![image-20200822113530032](E:\MapReduce\assets\image-20200822113530032.png)

## 具体代码编写

- 在之前的工程上创建模块为 MR
- 添加 pom.xml 依赖

```xml
<dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-core</artifactId>
            <version>2.7.6</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
                    <!--    <verbal>true</verbal>-->
                </configuration>
            </plugin>
            <!--
               <plugin>
                   <groupId>org.apache.maven.plugins</groupId>
                   <artifactId>maven-shade-plugin</artifactId>
                   <version>2.4.3</version>
                   <executions>
                       <execution>
                           <phase>package</phase>
                           <goals>
                               <goal>shade</goal>
                           </goals>
                           <configuration>
                               <minimizeJar>true</minimizeJar>
                           </configuration>
                       </execution>
                   </executions>
               </plugin>
       -->
        </plugins>
    </build>
```

### 在本地调试项目

编写map

新建一个wordcount.txt文本

```txt
hello,world,hadoop
hive,sqoop,flume,hello
kitty,tom,jerry,world
hadoop
```

本地放一个在hdfs上传一个

```sh
# 1.创建一个新的文件
cd /export/data
vim wordcount.txt

# 2.向其中放入以下内容并保存
hello,world,hadoop
hive,sqoop,flume,hello
kitty,tom,jerry,world
hadoop

# 3.上传到 HDFS
hdfs dfs -p  -mkdir  /input/wordcount
hdfs dfs -put wordcount.txt /input/wordcount
```





```java
package demo01;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author wp
 * Date 2021/5/4 13:21
 * Desc 1.实现读取文件 2.将数据进行map映射处理
 */
//首先要定义四个泛型的类型
//keyin:  LongWritable    valuein: Text
//keyout: Text            valueout:IntWritable
	
public class WordCoutMapperTask extends Mapper<LongWritable, Text,Text, IntWritable> {	 //map方法的生命周期：  框架每传一行数据就被调用一次
	//key :  这一行的起始点在文件中的偏移量
	//value: 这一行的内容
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("偏移量"+key.get());
        // 1. 读取每一行数据
        String line = value.toString();
        // 2. 判断行数据是否为空
        if(StringUtils.isNotEmpty(line)){
            // 3. 对数据进行切割
            String[] words = line.split(",");
            // 4. 对每个单词赋值为1
            for(String word : words){
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
}

```

```java
package demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduceTask extends Reducer<Text, IntWritable,Text,IntWritable> {	//生命周期：框架每传递进来一个kv 组，reduce方法被调用一次
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1.定义一个计数器
        int count = 0;
        // 2. 遍历所有值
        for (IntWritable v2 : values){
            count += v2.get();
        }
        // 3. 对对象累加值进行操作
        context.write(key,new IntWritable(count));
    }
}
```

```java
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
```

- 本地执行

```
（1）mapreduce程序是被提交给LocalJobRunner在本地以单进程的形式运行
（2）而处理的数据及输出结果可以在本地文件系统，也可以在hdfs上
（3）本地模式非常便于进行业务逻辑的调试
```



### 集群执行

```
（1）将mapreduce程序提交给yarn集群，分发到很多的节点上并发执行
（2）处理的数据和输出结果应该位于hdfs文件系统
```

- 修改的WordCountMain 类

```
#修改的地方
#1.参数 输入文件的参数，输出文件的参数，设置为 args[0] args[1]
#2. 在job里添加参数 job.setJarByClass(WordCountMain.class);
```

```java
package demo02;

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
        // 2. 指定job所在的jar包
        job.setJarByClass(WordCountMain.class);
        // 3. 读取数据输入组件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(args[0]));
        // 4. 设置maptask
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
        TextOutputFormat.setOutputPath(job,new Path(args[1]));
        //7 提交任务
        boolean flag = job.waitForCompletion(true);
        return flag?0:1;
    }
}
```

- 这个插件将这个jar包中所有依赖的第三方的 jar 包 导入到当前的jar包中，这个 jar 包就叫 肥包。
- 将这个 jar 包（不是带第三方依赖的小包）上传到 linux 中的任意目录（比如/root/）
- 在yarn集群中执行 mapreduce。 
  - 执行的脚本可以使用 hadoop jar （1.X） 也可以使用 yarn jar （2.x）运行。
  - 当前wordcount的集群执行命令

```sh
# 参数说明
# yarn jar 执行的命令
# original-day09_mapreduce1-1.0-SNAPSHOT.jar jar 包
# cn.itcast.mapreduce.WordCountMain 全路径类名
# /input/wordcount/wordcount.txt input参数
# /output/wordcount/ output参数
yarn jar original-MR-1.0-SNAPSHOT.jar demo02.mapreduce.WordCountMain /input/wordcount/wordcount.txt /output/wordcount/
```

