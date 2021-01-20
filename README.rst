********************************
Choice of Tools or Architecture
********************************

I used Hadoop with Spark since it offers advantages where data can be distributed across multiple name nodes and persisted. You also have resiliency since the data is replicated across nodes.

Another benefit we get is parallelism of the data processing. The Dataframes are essentially the complex structure of data fields and nested objects. Data Frames and Datasets, both of them are ultimately compiled down to an RDD

RDD
****
Resilient Distributed Datasets are collection of various data items that are partitioned across various nodes when they cannot fit in one node. A partition in spark is an atomic chunk of data (logical division of data) stored on a node in the cluster. Partitions are basic units of parallelism in Apache Spark. RDDs in Apache Spark are collection of partitions.

When were dealing with TB of data, we need a system where data is distributed. Spark on Hadoop offers both data distribution and paralle execution.

YARN
****



