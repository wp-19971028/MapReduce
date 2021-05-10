![image-20200825111925119](./assets\image-20200825111925119.png)



> MapTask的并行度指的是map阶段有多少个并行的task共同处理任务。map阶段的任务处理并行度，势必影响到整个job的处理速度。那么，MapTask并行实例是否越多越好呢？其并行度又是如何决定呢？
>
> 一个MapReducejob的**map阶段并行度由客户端在提交job时决定**，即客户端提交job之前会对待处理数据进行**逻辑切片**。切片完成会形成**切片规划文件（job.split）**，每个逻辑切片最终对应启动一个maptask。
>
> 逻辑切片机制由FileInputFormat实现类的**getSplits()**方法完成。

#### FileInputFormat切片机制

- 切片大小，默认等于block大小，即128M

- block是HDFS上物理上存储的存储的数据，切片是对数据逻辑上的划分。

- 在FileInputFormat中，计算切片大小的逻辑：

Math**.**max**(**minSize**,** Math**.**min**(**maxSize**,** blockSize**));**  

 ```
Math.max(minSize,Math.min(maxSize, blockSize));
 ```



- 切片举例

> 比如待处理数据有两个文件：
>
> file1.txt 320M
>
> file2.txt 10M	
>
> 经过FileInputFormat的切片机制运算后，形成的切片信息如下： 
>
> file1.txt.split1—0M~128M
>
> file1.txt.split2—128M~256M
>
> file1.txt.split3—256M~320M
>
> file2.txt.split1—0M~10M 

- FileInputFormat中切片的大小的由这几个值来运算决定：

> 在 FileInputFormat 中，计算切片大小的逻辑： 
>
> long splitSize = computeSplitSize(blockSize, minSize, maxSize)，
>
> 切片主要由这几个值来运算决定：
>
> **blocksize：**默认是 128M，可通过 dfs.blocksize 修改
>
> **minSize：**默认是 1，可通过 mapreduce.input.fileinputformat.split.minsize 修改
>
> **maxsize：**默认是 Long.MaxValue，可通过 mapreduce.input.fileinputformat.split.maxsize 修改
>
> 如果设置的最大值maxsize比blocksize值小，则按照maxSize切数据
>
> 如果设置的最小值minsize比blocksize值大，则按照minSize切数据
>
> 但是，不论怎么调参数，都不能让多个小文件“划入”一个 split

####  **FileInputFormat切片参数设置**

第一种情况（切片大小为256M）：

```java
FileInputFormat.setInputPaths(job, new Path(input));
FileInputFormat. setMaxInputSplitSize(job,1024*1024*500) ; //设置最大分片大小
FileInputFormat.setMinInputSplitSize(job,1024*1024*256); //设置最小分片大小
```

第二种情况(切片大小为100M)：

```java
FileInputFormat.setInputPaths(job, new Path(input));
FileInputFormat.setMaxInputSplitSize(job,1024*1024*100) ; //设置最大分片大小
FileInputFormat.setMinInputSplitSize(job,1024*1024*80); //设置最小分片大小
```

> 整个切片的核心过程在getSplit()方法中完成。
>
> 数据切片只是在逻辑上对输入数据进行分片，并不会再磁盘上将其切分成分片进行存储。InputSplit只记录了分片的元数据信息，比如起始位置、长度以及所在的节点列表等。

### **Reducetask并行度机制**

> reducetask并行度同样影响整个job的执行并发度和执行效率，与maptask的并发数由切片数决定不同，Reducetask数量的决定是可以直接手动设置：

job**.**setNumReduceTasks**(**4**);**

> 如果数据分布不均匀，就有可能在reduce阶段产生数据倾斜。
>
> 注意： reducetask数量并不是任意设置，还要考虑业务逻辑需求，有些情况下，需要计算全局汇总结果，就只能有1个reducetask。