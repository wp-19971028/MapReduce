## Mapreduce 的 分组操作

- 分区和分组的区别
  - 分区：将相同的 k2的数据，发送给同一个 reducer 中，这个操作是在 map端执行
  - 分组：将相同的 k2的值进行合并形成一个集合操作，在 reduce 中对同一个分区下的数据进行分组操作。
- 案例：需求：
  - 现在需要求出每一个订单中成交金额最大的一笔交易，将结果集存储到2个文件。
- 数据

| 订单id        | 商品id | 成交金额 |
| ------------- | ------ | -------- |
| Order_0000001 | Pdt_01 | 222.8    |
| Order_0000001 | Pdt_05 | 25.8     |
| Order_0000002 | Pdt_03 | 522.8    |
| Order_0000002 | Pdt_04 | 122.4    |
| Order_0000002 | Pdt_05 | 722.4    |
| Order_0000003 | Pdt_01 | 222.8    |

- 思路 
  - 如果使用 SQL 怎么写？
    - select  订单id,max(成交金额) from 订单表 group by 订单id

  - 使用mapreduce 怎么来实现呢？

![image-20200823152002153](./assets\image-20200823152002153.png)

```java
package demo07;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    //订单id
    private String orderid;
    //商品id
    private String pid;
    //成交金额
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderid, String pid, Double price) {
        this.orderid = orderid;
        this.pid = pid;
        this.price = price;
    }

    public String getOrderid() {
        return orderid;
    }

    public void setOrderid(String orderid) {
        this.orderid = orderid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }


    @Override
    public String toString() {
        return orderid + "\t" + pid + "\t" + price;
    }

    @Override
    public int compareTo(OrderBean orderBean) {
        int i = orderBean.orderid.compareTo(this.orderid);
        if (i == 0){
            int i1 = orderBean.price.compareTo(this.price);
            return  i1;
        }
        return i;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderid);
        out.writeUTF(pid);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderid = in.readUTF();
        this.pid = in.readUTF();
        this.price = in.readDouble();
    }
}
```

```java
package demo07;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition  extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        String orderid = orderBean.getOrderid();
        return (orderid.hashCode() & Integer.MAX_VALUE) % i;
    }
}
```

```java
package demo07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    public MyGroup() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;
        return  a1.getOrderid().compareTo(b1.getOrderid());
    }
}
```

```java
package demo07;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapperTask extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取每一行数据
        String line = value.toString();
        // 判断数据不为空
        if(StringUtils.isNotEmpty(line)){
            //2.1对数据切割
            String[] splits = line.split("\t");
            //2.2数据进行封装
            OrderBean orderBean = new OrderBean();
            orderBean.setOrderid(splits[0]);
            orderBean.setPid(splits[1]);
            orderBean.setPrice(Double.parseDouble(splits[2]));
            //将数据写出去
            context.write(orderBean,NullWritable.get());
        }
    }
}
```

```java
package demo07;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReduceTask extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //控制当前只取一条数据
        /**
         * reduce 就是将前面 map 分区 排序 分组得到的结果集拿到
         * for 循环，就会将所有的数据都输出到文件中
         * 这个时候用一个变量来确定，只输出一个值，
         * 来确保降序排列价格中最高的值
         */
        //size 代表当前我要输出到文件的订单个数
        //int size = 1;
        //length 当前的输出的个数
        int length = 1;
        for (NullWritable value : values) {
            context.write(key, value);
            if (length == 1) {
                break;
            }
            //length++;
        }
    }
}
```

```java
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
```

