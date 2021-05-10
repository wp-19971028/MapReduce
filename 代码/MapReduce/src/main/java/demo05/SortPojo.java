package demo05;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortPojo implements WritableComparable<SortPojo> {
    private String first;
    private int second;

    public SortPojo() {
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }


    @Override
    public String toString() {
        return first + "\t" +second;
    }

    /**
     * 排序的方法，如果第一列不相等，根据字典顺序降序排列
     * 如果第一列相同的清空下，第二列升序排列
     * 如果 this.first.comparaTo(o.first)  升序排列
     * 如果 o.first.comparaTo(this.first)  降序排列
     * @param o
     * @return
     */
    @Override
    public int compareTo(SortPojo o) {
        //先对第一列排序: Word排序
        int result = this.first.compareTo(o.first);
        //如果第一列相同，则按照第二列进行排序
        if(result == 0){
            return  this.second - o.second;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }
}