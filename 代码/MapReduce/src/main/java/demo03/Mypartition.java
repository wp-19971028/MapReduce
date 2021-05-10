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
