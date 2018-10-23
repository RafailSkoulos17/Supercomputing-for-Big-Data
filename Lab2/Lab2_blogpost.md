# ***Lab 2 blog post***
## ***Super Computing for Big Data***
### *Group 4*
### *Kyriakos Psarakis (4909437)*
### *Rafail Skoulos (4847482)*  
&nbsp;
### **Initial Run**

In this lab we had to chose between our two implementations we had from Lab1, RDD and Dataframe. We began by running our code on AWS, starting with data from 1 month and gradually increasing them. At the beginning, RDD seemed to be the optimal choice. However when we tried to run RDD implementation for the whole dataset (about 4TBs) we got a memory limit error on our resource manager. So, our first thought was to check the cluster’s configuration and see why we had this error. What we observed was that if we did not specify the configuration to the cluster, it uses the defaults which are insufficient for our big data problem. Consequently, we searched on the [AWS EMR documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-sparkconfigure.html) and found that if we set the ***maximizeResourceAllocation*** parameter setting to true. the cluster will automatically configure Spark to run with optimized settings based on our cluster instances. Namely, creating one executor per Node and giving him most of the Nodes resources.
&nbsp;
### **Framework choice**

Now that both of our implementations run on the cluster for the entire dataset, we need to choose between them. We compared the runtime for the two implementations and we found out that the runtime for RDD exceeded the minimum of 30 minutes while the one for Datafrme was 8.5 minutes. That led us to chose to improve upon the Dataframe implementation for Lab2. The only change we did to the code form Lab1 was to remove the line that printed the results on the driver node and replace it with the following line: `ds2.write.format("json").save(args(1))` that saves each nodes results in a distributed way into Amazons S3.
&nbsp;
### **Initial Measurements**

From our initial run we got these results:

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/only%20maximise%20large/Screenshot%20from%202018-10-19%2020-38-38.png?raw=true)

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/only%20maximise%20large/Screenshot%20from%202018-10-19%2020-38-15.png?raw=true)
As we can conclude from the images above:
- The runtime for the entire dataset is 8.5 minutes.
- We have 20 executors, 1 per Node.
- The garbage collection time is 19,4%, bigger than the normal of 10%. This is a drawback of having executors with so much RAM.
- The Shuffle read/write are 61.7 GB, 1.5% of our 4TB dataset. This rate is good, given that for our use case are required 2 wide dependency transformations in order to reach our desirable outcome.
- The overall maximum cost of this run is: (0.3 (cost per node) *20(#nodes)) ($/h) *(8.5(runtime mins)/60(mins/h)) (h) = 0.85$.


The next step is to check how our cluster’s resources get utilized in order to detect possible bottlenecks and try to find improvements to tackle them. To start with, we check in the two following screenshots of the CPU utilization of our Nodes.

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/only%20maximise%20large/Screenshot%20from%202018-10-19%2020-39-09.png?raw=true)

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/only%20maximise%20large/Screenshot%20from%202018-10-19%2020-40-30.png?raw=true)

From the images above is clear that:
- We don’t fully utilize our cluster’s power, running at about 70%.
- The load is evenly distributed across all the nodes. 

This could be improved if we increase the number of tasks that run on each Node. However, it is not that simple due to the case that the bottleneck could be on another component of the cluster. The most common one is the network connection, which may be unable to feed data to our executors fast enough and as a result it leads to lower CPU usage than  maximum.

The next two screenshots show the load of the cluster and RAM usage.

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/only%20maximise%20large/Screenshot%20from%202018-10-19%2020-39-43.png?raw=true)

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/only%20maximise%20large/Screenshot%20from%202018-10-19%2020-39-26.png?raw=true)

Those two are as expected, due to the 70% CPU usage and the RAM is related to the task’s input size.
&nbsp;
In the last screenshot from Ganglia, we have the component with the higher probability of being the bottleneck. We use the term 'probability' because Amazon does not state their cluster’s network speed. We can observe that it sit around the 10Gigabit mark and it is safe to assume that the connection is the main bottleneck to our application.

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/only%20maximise%20large/Screenshot%20from%202018-10-19%2020-39-58.png?raw=true)

&nbsp;
### **Code Optimizations**

In this section we will present the optimizations we tried in our code.
#### Kryo Serializer
The first thing that we have found when we where searching for a way to optimize our code's performance was to introduce to our code the Kryo serializer. We made that change but the results were the same (or even worse). Then with further search we found in [Apache Spark Documentation](https://spark.apache.org/docs/2.1.0/sql-programming-guide.html) that the Dataset API uses its own “specialized encoder to serialize the objects for processing or transmitting over the network, fact that explained our results.

#### Avoid complex UDFs
Another highly suggested optimization for the Dataset API is to replace the complex UDFs. We have only used one complex UDF that transformed the final output data to our desired format. After removing it, the runtime did not change so, we chose to keep it.

#### Cache
Then we tried to improve our application’s performance by inspecting if we can use some memory only for caching. However, due to fact that our DAG was just a straight line of transformations and no intermediate results were reused again from any other part in our application, the results of having cached a transformation in our DAG were the same as the ones from original run.

#### Parallel writes to S3
The next improvement in our mind was to improve the way our results were outputted. In the beginning we were just printing them to our driver node but we needed to keep them in files in order to persist them. That led to us collecting all the results to the driver node, merging them and then writing them in a single file. However, that was not the optimal way to write the output to S3 so we changed it by making every node writing its results to S3 independently. That led to a big improvement in performance of around 35%. In addition, if we wanted the results in a single file we could just download them and concatenate the files. Finally, we chose to write the output to S3 because it is native to the Amazon’s environment and as a result it is providing the best write speeds. That is the reason we didn’t try a different file system like HDFS.

### **EMR Cloud Optimizations**

In this section we will present the ways we used to fine tune our cluster’s configuration in order to improve its performance.

#### Double the amount of executors

An improvement that we thought about was to increase the parallel computation. So we decided to double the number of executors per node. To do that we split the resources of each node in half so, each executor would have equal computational power. As we can see in the following screenshots:
- We achieved a 9.52% improvement in runtime.
- The garbage collection time dropped from 19.4% to 10.2% due to the decrease of the memory pool of each executor. 

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/40c44/Screenshot%20from%202018-10-20%2016-20-15.png?raw=true)

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/40c44/Screenshot%20from%202018-10-20%2016-20-40.png?raw=true)

Furthermore, the following graphs in ganglia showed that as expected better CPU and memory utilization. Especially:
- The CPU rose from 70% to 85%.
- The memory use increased by 30 Gb. 
- The 10Gigabit network bottleneck still remained. 

The cost of this run is 0.76$.

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/40c44/Screenshot%20from%202018-10-20%2016-20-59.png?raw=true)

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/40c44/Screenshot%20from%202018-10-20%2016-21-14.png?raw=true)

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/40c44/Screenshot%20from%202018-10-20%2016-21-40.png?raw=true)

Finally, we should mention that this divide and conquer approach does not work indefinitely. We observed that if we quadruple the number of executor, performance issues begin to arise which are leading to a substantial decrease in performance. The cause of this is that we decrease the computational power of our nodes a lot and in addition to that we increase the communication between those executors. Moreover, the further increase of the executor may  cause an OutOfMemoryExeption like those in our initial run before setting maximize resource allocation to true.

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/80/Screenshot%20from%202018-10-20%2017-43-01.png?raw=true)

![](https://github.com/RafailSkoulos/SCBD_lab2_blogpost/blob/master/group4images/80/Screenshot%20from%202018-10-20%2017-42-46.png?raw=true)


### Metric to Improve

In Lab1s report the metric that we chose to improve was the price to performance ratio of our implementation. The way that we approached this was by decreasing the number of nodes in order to decrease the price. However, that also decreased performance. Finally, we can chose a cluster setup that suits best our price and time constraints.


Dataset | RunTime | #Nodes | #Executors | Price/Node/Hour|  Run’sCost | $/TB
------- | ------- | ------ | ---------- | -------------- | ---------- | ----
4 TB | 8.5 min |  20 |  20 | 0.3 $/h |  0.85$ | 0.21
4 TB | 7.6 min | 20 | 40 | 0.3 $/h | 0.76$ | 0.19
4 TB | 10.1 min | 15 | 30 | 0.3 $/h | 0.75$ | 0.189

From these runs, it is safe to conclude that in our use case the best option is the one with the 20 nodes with the improvement of the executor increase. The approach to reduce the number of nodes in order to decrease the price is baseless, as we see that the price decreases by less than 1% and at the same time the runtime plummets to a 24.7% decrease. In conclusion, the best price to performance ratio in our case is to lease the 20 Nodes that will finish the job faster costing less due to less time it takes them to finish the task.

### Future improvements

A future improvement in respect to our code,  could be to try to not create so much garbage, in order to decrease the time it takes to the garbage collector to do its job. Additionally, we tried but did not manage to find a way to change the default garbage collector inside Amazon’s EMR, which might have led to performance increase. Finally, given the numerous options that exists in spark configuration, we can still make some minor tweaks in some little-known configuration parameters that could possibly lead to a minor performance increase.

