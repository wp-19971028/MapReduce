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
