# ET4310 Supercomputing for Big Data
For this assignment, we had to process the entire [GDELT](https://www.gdeltproject.org/) dataset to analyze the 10 most common topics in the news for each day. The entire dataset consists of several terabytes, so a Spark application was made to do this analysis using Amazon Elastic MapReduce (EMR).

In this blog post, we explain how we got to run our Spark application on the entire dataset using 20 c4.8xlarge spot instances under half an hour and how we managed to optimize the Spark application for our chosen metric. 

In the end, the two best configurations were:
- 3x c5.18xlarge spot instances: 20 minutes and 4 seconds, $1.01.
- 10x c5.18xlarge spot instances: 7 minutes and 14 seconds, $1.18.

## Metric

For companies it is not only important that an application can run in a short period of time, but also that running the application is not too expensive. A suitable metric for our Spark application is to measure the cost to perform the analysis on the entire GDELT dataset, but having a time limit of 30 minutes. This time limit ensures that using clusters that are low in price but take many hours to complete the analysis is not a viable option. To calculate the cost of running the application on Amazon EMR, we used the following formula:

_**Total cost** = **elapsed time** x (**master node price** + **instance counts** x (**Amazon EC2 price** + **Amazon EMR price**))_

We measured the elapsed time of the Spark application step and calculated the total cost including the cost of using one on-demand m4.large instance as the master node ($0.130/hr). All experiments were conducted on the US East (Ohio) server, which is a different region than where the S3 bucket of the GDELT dataset is located (US East (N. Virginia)). The spot instance prices were significantly lower for the US East (Ohio) server. Additional costs such as transferring data between regions were left out of consideration.

## Baseline for processing entire dataset: 9.37 min, $1.77.
We decided to use the RDD implementation instead of the Dataframe/Dataset implementation, because the former ran faster on our local computers and AWS. To make the RDD implementation from the first lab work on AWS, two things had to be modified in the code. First of all, the SparkSession config option _.config(”spark.master”, ”local”)_ had to be removed. Secondly, an optional argument was added to our application to specify the path to the GDELT S3 bucket for reading the segments.

After first testing with a limited number of segments, we managed to succesfully process the entire dataset with 20 c4.8xlarge instances in less than 10 minutes for a total cost of $1.77. The following Spark-submit options were used:

__*--num-executors 139 --executor-cores 5 --executor-memory 7G --driver-memory=10G --conf spark.default.parallelism=695*__

These Spark-submit options were derived for 20 c4.8xlarge instances (36 vCPUs and 60 GiB of memory per instance): 
- executor-cores: this was set to 5, as more concurrent tasks per executor can lead to bad HDFS I/O throughput [3,4].
- num-executors: using 5 cores for each executor, the number of executors can be calculated: (36 vCPUs - 1 vCPU for system processes) / 5 executor-cores = 7 executors per node. For 20 nodes, this will be 20 x 7 = 140. Reserving one core for the driver node: 140 - 1 = 139.

- executor-memory: (60 - 1 for system processes) / 7 executors per node = 8.4 per node. 8.4 - 7% off heap overhead = 7.812, rounded down to 7.

- driver-memory: collect() brings all the data from the executors to the driver, so we need more memory for the driver. (60 - 1 for system processes) - (7 executors per node * 7 memory) = 10.

- spark.default.parallelism: number of executors x cores per executor = 139 x 5 = 695. This parameter divides the data into partitions and should be tuned to the data and available hardware.

## Improvements: 9.37 min, $1.77 --> 20.07 min, $1.01.

We managed to reduce the total cost of processing the entire dataset from $1.77 to $1.01, while keeping the run time from exceeding 30 minutes. The results are shown in the table below:

| Approach                      | Instance type | Instance count | EC2 Spot ($/hr) | EMR ($/hr) | Master node ($/hr) | Elapsed time (s) | Elapsed time (min) | Total cost ($) |
|-------------------------------|:-------------:|:--------------:|:---------------:|:----------:|:------------------:|:----------------:|:------------------:|:--------------:|
| Baseline                      |   c4.8xlarge  |       20       |      0.290      |    0.270   |        0.130       |        562       |        9.37        |      1.77      |
| Changed to Kryo serialization |   c4.8xlarge  |       20       |      0.290      |    0.270   |        0.130       |        526       |        8.77        |      1.66      |
| Changed parallelism to 200    |   c4.8xlarge  |       20       |      0.290      |    0.270   |        0.130       |        476       |        7.93        |      1.50      |
| Switched to C5 instances      |  c5.18xlarge  |       10       |      0.696      |    0.270   |        0.130       |        434       |        7.23        |      1.18      |
| Downsized to 3 core nodes     |  c5.18xlarge  |        3       |      0.696      |    0.270   |        0.130       |       1204       |        20.07       |      1.01      |

### Changing Java serialization to Kryo serialization
We changed the default Java serialization to Kryo serialization by adding the following line to the Spark-submit options:

__*--conf spark.serializer=org.apache.spark.serializer.KryoSerializer*__

This saved 36 seconds, as Kryo serialization leads to smaller serialized objects, which means that less memory will be used and that less data has to be transferred. The Kryo serializer is also faster than the Java serializer.

### Tuning parallelism level
Lowering the *spark.default.parallelism* property from 695 to 200 made the Spark application step complete faster by 50 seconds. Tuning this parameter leads to better CPU utilization, by adjusting the number of partitions in RDDs returned by transformations (e.g. reduceByKey).

### Switching to C5 instances
Amazon EC2 C5 is a newer generation of the C4 compute optimized instances, and includes a larger size: c5.18xlarge. This instance type provides 72 vCPUs and 144 GiB of memory. Although the spot instance price is 2.4 times more expensive than that of a c4.8xlarge instance, the EMR price per hour is the same: $0.270. This makes the c5.18xlarge more cost effective than a c4.8xlarge instance. 

By using 10 c5.18xlarge instead of 20 c4.8xlarge instances, the run time decreased further from 476 to 432 seconds. The total cost was also lowered from $1.50 to $1.18. For these C5 instances, the Spark-submit options were tuned in the same procedure as outlined in the baseline.

### Downsizing the cluster
Our chosen metric allows for using instance types with less compute power or using fewer instances, as long as the analysis is performed within 30 minutes. The best approach thus far was using 10 c5.18xlarge instances with a run time of roughly 7 minutes.

We found that using 3 c5.18xlarge increased the run time (although not proportionally), but reduced the cost by $0.17. This means that there is some overhead when running the application with more instances.

This was the best outcome that we could achieve, but we think that the slightly lower cost of using fewer instances do not justify the additional duration of the Spark application. When using spot instances, it is recommended to do the analysis in the least amount of time, to prevent interruptions of a spot instance by EC2.

## What did not work
- Filtering of false positives. We noticed many false positives in the output of the analysis, for example "Type ParentCategory", "Capistrano Unified School District" and "Audio Described". In the RDD code, we added filters for the 10 most common false positives. This only reduced the run time by 2 seconds. We think that this could be the result of doing too many passes over the RDD.

- Using more parallelism. We experimented with different _spark.default.parallelism_ levels (2000, 695, 200, 100). This is a difficult parameter to tune, as setting it to suboptimal levels resulted in long run times or even crashes. 

- Using only 2 core nodes of c5.18xlarge instance type, which made the application crash after 30 minutes. This could be due to having a suboptimal parallelism level.

## References
1. https://spark.apache.org/docs/latest/configuration.html
2. https://spark.apache.org/docs/latest/tuning.html
3. http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
4. https://rea.tech/how-we-optimize-apache-spark-apps/
