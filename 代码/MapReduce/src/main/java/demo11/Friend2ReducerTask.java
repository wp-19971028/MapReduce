package demo11;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Friend2ReducerTask extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String v3="";
        //1.遍历
        for(Text value:values){
            v3 += value+",";
        }
        //2.写出去
        context.write(key, new Text(v3));
    }
}
