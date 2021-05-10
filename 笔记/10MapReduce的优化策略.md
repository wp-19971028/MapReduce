# MapReduce性能优化策略

> 使用Hadoop进行大数据运算，当数据量极其大时，那么对MapReduce性能的调优重要性不言而喻，尤其是Shuffle过程中的参数配置对作业的总执行时间影响特别大。下面总结一些和MapReduce相关的性能调优方法，主要从五个方面考虑：数据输入、Map阶段、Reduce阶段、Shuffle阶段和其他调优属性。

## 数据输入

> 在执行MapReduce任务前，将小文件进行合并，大量的小文件会产生大量的map任务，增大map任务装载的次数，而任务的装载比较耗时，从而导致MapReduce运行速度较慢。因此我们采用CombineTextInputFormat来作为输入，解决输入端大量的小文件场景。

## Map阶段

 (1）减少溢写（spill）次数：通过调整io.sort.mb及sort.spill.percent参数值，增大触发spill的内存上限，减少spill次数，从而减少磁盘IO。

（2）减少合并（merge）次数：通过调整io.sort.factor参数，增大merge的文件数目，减少merge的次数，从而缩短mr处理时间。

（3）在map之后，不影响业务逻辑前提下，先进行combine处理，减少 I/O。

我们在上面提到的那些属性参数，都是位于mapred-default.xml文件中，这些属性参数的调优方式如表所示。



| **属性名称**                              | **类型** | **默认值** | **说明**                                                     |
| ----------------------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapreduce.task.io.sort.mb                 | int      | 100        | 配置排序map输出时使用的内存缓冲区的大小，默认100Mb，实际开发中可以设置大一些。 |
| mapreduce.map.sort.spill.percent          | float    | 0.80       | map输出内存缓冲和用来开始磁盘溢出写过程的记录边界索引的阈值，即最大使用环形缓冲内存的阈值。一般默认是80%。也可以直接设置为100% |
| mapreduce.task.io.sort.factor             | int      | 10         | 排序文件时，一次最多合并的流数，实际开发中可将这个值设置为100。 |
| mapreduce.task.min.num.spills.for.combine | int      | 3          | 运行combiner时，所需的最少溢出文件数(如果已指定combiner)     |

## Reduce阶段

（1）合理设置map和reduce数：两个都不能设置太少，也不能设置太多。太少，会导致task等待，延长处理时间；太多，会导致 map、reduce任务间竞争资源，造成处理超时等错误。

（2）设置map、reduce共存：调整slowstart.completedmaps参数，使map运行到一定程度后，reduce也开始运行，减少reduce的等待时间。

（3）规避使用reduce：因为reduce在用于连接数据集的时候将会产生大量的网络消耗。通过将MapReduce参数setNumReduceTasks设置为0来创建一个只有map的作业。

（4）合理设置reduce端的buffer：默认情况下，数据达到一个阈值的时候，buffer中的数据就会写入磁盘，然后reduce会从磁盘中获得所有的数据。也就是说，buffer和reduce是没有直接关联的，中间多一个写磁盘->读磁盘的过程，既然有这个弊端，那么就可以通过参数来配置，使得buffer中的一部分数据可以直接输送到reduce，从而减少IO开销。这样一来，设置buffer需要内存，读取数据需要内存，reduce计算也要内存，所以要根据作业的运行情况进行调整。

我们在上面提到的属性参数，都是位于mapred-default.xml文件中，这些属性参数的调优方式如表所示。

| **属性名称**                                 | **类型** | **默认值** | **说明**                                                     |
| -------------------------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapreduce.job.reduce.slowstart.completedmaps | float    | 0.05       | 当map task在执行到5%，就开始为reduce申请资源。开始执行reduce操作，reduce可以开始拷贝map结果数据和做reduce shuffle操作。 |
| mapred.job.reduce.input.buffer.percent       | float    | 0.0        | 在reduce过程，内存中保存map输出的空间占整个堆空间的比例。如果reducer需要的内存较少，可以增加这个值来最小化访问磁盘的次数。 |

## Shuffle阶段

Shuffle阶段的调优就是给Shuffle过程尽量多地提供内存空间，以防止出现内存溢出现象，可以由参数mapred.child.java.opts来设置，任务节点上的内存大小应尽量大。

我们在上面提到的属性参数，都是位于mapred-site.xml文件中，这些属性参数的调优方式如表所示。

| **属性名称**                  | **类型** | **默认值** | **说明**                                                     |
| ----------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapred.map.child.java.opts    |          | -Xmx200m   | 当用户在不设置该值情况下，会以最大1G jvm heap size启动map task，有可能导致内存溢出，所以最简单的做法就是设大参数，一般设置为-Xmx1024m |
| mapred.reduce.child.java.opts |          | -Xmx200m   | 当用户在不设置该值情况下，会以最大1G jvm heap size启动Reduce task，也有可能导致内存溢出，所以最简单的做法就是设大参数，一般设置为-Xmx1024m |

## 其他调优属性

除此之外，MapReduce还有一些基本的资源属性的配置，这些配置的相关参数都位于mapred-default.xml文件中，我们可以合理配置这些属性提高MapReduce性能，表4-4列举了部分调优属性。

| **属性名称**                            | **类型** | **默认值** | **说明**                                                     |
| --------------------------------------- | -------- | ---------- | ------------------------------------------------------------ |
| mapreduce.map.memory.mb                 | int      | 1024       | 一个Map Task可使用的资源上限。如果Map Task实际使用的资源量超过该值，则会被强制杀死。 |
| mapreduce.reduce.memory.mb              | int      | 1024       | 一个Reduce Task可使用的资源上限。如果Reduce Task实际使用的资源量超过该值，则会被强制杀死。 |
| mapreduce.map.cpu.vcores                | int      | 1          | 每个Map task可使用的最多cpu core数目                         |
| mapreduce.reduce.cpu.vcores             | int      | 1          | 每个Reduce task可使用的最多cpu core数目                      |
| mapreduce.reduce.shuffle.parallelcopies | int      | 5          | 每个reduce去map中拿数据的并行数。                            |
| mapreduce.map.maxattempts               | int      | 4          | 每个Map Task最大重试次数，一旦重试参数超过该值，则认为Map Task运行失败 |
| mapreduce.reduce.maxattempts            | int      | 4          | 每个Reduce Task最大重试次数，一旦重试参数超过该值，则认为Map Task运行失败 |

