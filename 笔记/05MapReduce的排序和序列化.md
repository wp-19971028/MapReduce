# 概述

- 序列化（Serialization）是指把结构化对象转化为字节流。
- 反序列化 （Deserialization）是序列化的逆过程。把字节流转为结构化对象。



> 当要在进程间传递对象或持久化对象的时候，就需要序列化对象成字节流，反之当要将接收到或从磁盘读取的字节流转换为对象，就要进行反序列化。
>
> Java的序列化（Serializable）是一个重量级序列化框架，一个对象被序列化后，会附带很多额外的信息（各种校验信息，header，继承体系…），不便于在网络中高效传输；所以，hadoop自己开发了一套序列化机制（**Writable**），精简，高效。不用像java对象类一样传输多层的父子关系，需要哪个属性就传输哪个属性值，大大的减少网络传输的开销。
>
> Writable是Hadoop的序列化格式，hadoop定义了这样一个Writable接口。
>
> 一个类要支持可序列化只需实现这个接口即可。

```java
public interface  Writable {
 void write(DataOutput out) throws IOException;
 void readFields(DataInput in) throws IOException;
}
```

> 另外 Writable 有一个子接口是 WritableComparable, WritableComparable 是既可实现序列化, 也可以对key进行比较, 我们这里可以通过自定义 Key 实现 WritableComparable 来实现我们的排序功能.

```java
// WritableComparable分别继承Writable和Comparable
public interface WritableComparable<T> extends Writable, Comparable<T> {
}
//Comparable
public interface Comparable<T> {
    int compareTo(T var1);
}
```

Comparable接口中的comparaTo方法用来定义排序规则，用于将当前对象与方法的参数进行比较。

例如：o1.compareTo(o2);

​	如果指定的数与参数相等返回0。

​	如果指定的数小于参数返回 -1。

​	如果指定的数大于参数返回 1。

返回正数的话，当前对象（调用compareTo方法的对象o1）要排在比较对象（compareTo传参对象o2）后面，返回负数的话，放在前面。

案例：

- 需求： 将数据进行排列，第一列降序排列，如果相等的情况下，第二列进行升序排列，输出到文件。

实现SortPojo 类 思路：

- **实现自定义的bean来封装数据**，并将bean作为map输出的key来传输
- MR程序在处理数据的过程中会对数据排序(map输出的kv对传输到reduce之前，会排序)，排序的依据是map输出的key。所以，我们如果要实现自己需要的排序规则，则可以考虑将排序因素放到key中，让key实现接口：WritableComparable，然后重写key的compareTo方法。
- SortPojo类

```java
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
```

