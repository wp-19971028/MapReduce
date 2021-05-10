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
