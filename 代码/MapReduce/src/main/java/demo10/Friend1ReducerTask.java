package demo10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Friend1ReducerTask extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String v3 = "";
        // 1. 遍历好友信息列表
        for (Text friend:values) {
            v3 += friend + "-";
        }
        // 2. 写出去
        context.write(key,new Text(v3));
    }
}
