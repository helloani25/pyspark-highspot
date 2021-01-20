********************************
# Choice of Tools or Architecture
********************************

I used Hadoop with Spark since it offers advantages where data can be distributed across multiple name nodes and persisted. You also have resiliency since the data is replicated across nodes.

Another benefit we get is parallelism of the data processing. The Dataframes are essentially the complex structure of data fields and nested objects. Data Frames and Datasets, both of them are ultimately compiled down to an RDD

### RDD
****
Resilient Distributed Datasets are collection of various data items that are partitioned across various nodes when they cannot fit in one node. A partition in spark is an atomic chunk of data (logical division of data) stored on a node in the cluster. Partitions are basic units of parallelism in Apache Spark. RDDs in Apache Spark are collection of partitions.

When were dealing with TB of data, we need a system where data is distributed. Spark on Hadoop offers both data distribution and paralle execution.

### YARN
****
YARN is a generic resource-management framework for distributed workloads; in other words, a cluster-level operating system.

![Alt text](images/Yarn-Architecture.gif?raw=true "Yarn Architecture")

The ResourceManager and the NodeManager form the data-computation framework. 

The ResourceManager is the ultimate authority that arbitrates resources among all the applications in the system. The NodeManager is the per-machine agent who is responsible for containers, monitoring their resource usage (cpu, memory, disk, network) and reporting the same to the ResourceManager/Scheduler.

The per-application ApplicationMaster is, in effect, a framework specific library and is tasked with negotiating resources from the ResourceManager and working with the NodeManager(s) to execute and monitor the tasks.

So using YARN we can schedule tasks to modify large files and specify the resource constraints

##### Spark Parameters to tune to have better resource utilization in production
**********************************************************************
<B>Note</B>: I am not running Spark job in Cluster mode using YARN for this exercise
##### In production
```
Edit $SPARK_HOME/conf/spark-defaults.conf and set spark.master to yarn:
```

###### yarn.nodemanager.resource.memory-mb

It is the amount of physical memory, in MB, that can be allocated for containers in a node. This value has to be lower than the memory available on the node.

###### yarn.scheduler.minimum-allocation-mb
It is the minimum allocation for every container request at the ResourceManager, in MBs. In other words, the ResourceManager can allocate containers only in increments of this value

###### yarn.scheduler.increment-allocation-mb 
properties control the minimum and increment request values respectively.

###### yarn.scheduler.maximum-allocation-mb
The maximum allocation for every container request at the ResourceManager, in MBs.


###### spark.executor.memory
Since every executor runs as a YARN container, it is bound by the Boxed Memory Axiom.
In cluster deployment mode, since the driver runs in the ApplicationMaster which in turn is managed by YARN, this property decides the memory available to the ApplicationMaster, and it is bound by the Boxed Memory Axiom.

###### yarn.nodemanager.resource.cpu-vcores
controls the maximum sum of cores used by the containers on each node.

###### executor-cores
The number of cores can be specified with  invoking spark-submit, spark-shell,

###### num-executors command-line flag or spark.executor.instances 
configuration property control the number of executors requested

#### Installing HADOOP
* Download https://archive.apache.org/dist/hadoop/common/hadoop-3.0.3/hadoop-3.0.3.tar.gz

* Move the downloaded Hadoop binary to below path & extract it.
$HOME/BigData

* please make changes (as mentioned in detail below) in the following files under $HADOOP_HOME/etc/hadoop/
core-site.xml
```
etc/hadoop/core-site.xml:
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

hdfs-site.xml
```
etc/hadoop/hdfs-site.xml:
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```
mapred-site.xml
```
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
```
yarn-site.xml
```
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
```
hadoop-env.sh
```
# export JAVA_HOME [Same as in .profile file]
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_141.jdk/Contents/Home

# Location of Hadoop.
export HADOOP_HOME=[Download hadoop Binary path]

```

* Enable localhost

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

* Try the following command:
```
$ bin/hadoop
```
This will display the usage documentation for the hadoop script.

* Add ENV variable to bash profile

```
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

export HADOOP_HOME=$HOME/BigData/hadoop-3.0.3
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export YARN_HOME=$HADOOP_HOME
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME
export HADOOP_LIBEXEC_DIR=$HADOOP_HOME/libexec
export JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_LIBRARY_PATH
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_INSTALL=$HADOOP_HOME

export SCALA_HOME=/usr/local/opt/scala@2.11/bin

export PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME:$SPARK_HOME/bin:$SPARK_HOME/sbin

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_HOME=$HOME/BigData/spark-3.0.1-bin-hadoop3.2

alias hstart=$HADOOP_HOME/sbin/start-all.sh
alias hstop=$HADOOP_HOME/sbin/stop-all.sh

alias start-yarn=$HADOOP_HOME/sbin/start-yarn.sh
alias stop-yarn=$HADOOP_HOME/sbin/stop-yarn.sh

alias start-dfs=$HADOOP_HOME/sbin/start-dfs.sh
alias stop-dfs=$HADOOP_HOME/sbin/stop-dfs.sh
export PATH="/usr/local/opt/scala@2.11/bin:$PATH"
alias python=/usr/local/bin/python3

export PATH="$HOME/.poetry/bin:$PATH"
```

* Format HDFS

1. Format the filesystem: 
```
$ bin/hdfs namenode -format
```
2. Start NameNode daemon and DataNode daemon:  
```
$ sbin/start-dfs.sh 
```
The hadoop daemon log output is written to the $HADOOP_LOG_DIR directory (defaults to $HADOOP_HOME/logs).

* Start ResourceManager daemon and NodeManager daemon:
 ```
$ sbin/start-yarn.sh
```
* To verify that the namenode and datanode daemons are running, execute the following command on the terminal. This displays running Java processes on the system.
```
$ jps
```
* Check if the installation is successful

a. Resource Manager: http://localhost:9870
b. JobTracker: http://localhost:8088/
c. Node Specific Info: http://localhost:8042/

* Setup temp directory for data and logs

```
hdfs dfs -mkdir /tmp
hdfs dfs -chmod g+w /tmp
```
<table>
<tr><th>Dir Name</th><th> Location</th></tr>
<tr>
<td> dfs.namenode.name.dir</td><td>file://${hadoop.tmp.dir}/dfs/name</td>	
</tr>
<tr>
<td>dfs.datanode.data.dir</td><td>	file://${hadoop.tmp.dir}/dfs/data</td>
</tr>
</table>

On my machine tmp location is   /tmp/hadoop-anithasubramanian

#### Installing Spark

* Download Spark
https://spark.apache.org/downloads.html 
spark-3.0.1-bin-hadoop3.2.tgz

* Extract to $HOME/BigData

* On mac brew install scala@2.11   

* At this point you should be able to run spark-shell or pyspark:
Check if the link below works
http://localhost:4040/

##### Note: 
1. You need Java 8 for this installation.
2. Hive could be installed to save the files with better compression using parquet tables. For this exercise I have not installed it.
3. I am running in single node mode. In production we must run in cluster mode using YARN.
##### Set up for this project
```
hdfs dfs -mkdir /user/anithasubramanian/outputs
hdfs dfs -mkdir /user/anithasubramanian/inputs
 ```

Add the mixtape.json to /user/anithasubramanian/inputs
```
hdfs dfs -put inputs/mixtape.json /user/anithasubramanian/inputs
hdfs dfs -put inputs/edit.json  /user/anithasubramanian/inputs
 ```

Deleteing the output directory
```
hdfs dfs -rm -r  /user/anithasubramanian/outputs/mixtape.json
```
##### How to run
spark-submit pyspark_highspot/sparksession.py

Result will be saved to the /user/anithasubramanian/outputs/mixtape.json folder
```
 hdfs dfs -ls /user/anithasubramanian/outputs/mixtape.json
2021-01-20 14:59:04,245 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 2 items
-rw-r--r--   1 anithasubramanian supergroup          0 2021-01-20 14:58 /user/anithasubramanian/outputs/mixtape.json/_SUCCESS
-rw-r--r--   1 anithasubramanian supergroup       2554 2021-01-20 14:58 /user/anithasubramanian/outputs/mixtape.json/part-00000-a046c4a5-7ccc-4083-a2cb-2cca8fa89464-c000.json

```
In this case /user/anithasubramanian/outputs/mixtape.json/part-00000-a046c4a5-7ccc-4083-a2cb-2cca8fa89464-c000.json has the result

##### Considerations made for edit.json

Edit json has separate sections for insert = create key , delete and update separately.
1. Only the playlist id that do not match any existing playlists will be inserted and the rest will be ignored
2. Only the playlist ids that match the existing playlist ids will be deleted and rest will be ignored
3. Only the playlist ids and user ids that exists in the source playlist will be matched to add an existing song. Any song
that does not exist will be ignored.

```

  "create": {
    "playlists": [
      {
        "id": "4",
        "user_id": "1",
        "song_ids": [
          "8",
          "32"
        ]
      }
    ]
  },
  "delete": {
    "playlist_ids": ["4","2"]
  },
  "update": {
    "playlists": [
      {
        "id": "2",
        "user_id": "3",
        "song_ids": [
          "13",
          "14"
        ]
      }
    ]
  }
}
```

#### Production set up for inputs files

The Optimized Row Columnar (ORC) file format provides a highly efficient way to store Hive data. It was designed to overcome limitations of the other Hive file formats. Using ORC files improves performance when Hive is reading, writing, and processing data.

Compared with RCFile format, for example, ORC file format has many advantages such as:

a single file as the output of each task, which reduces the NameNode's load
Hive type support including datetime, decimal, and the complex types (struct, list, map, and union)
light-weight indexes stored within the file
skip row groups that don't pass predicate filtering
seek to a given row
block-mode compression based on data type
run-length encoding for integer columns
dictionary encoding for string columns
concurrent reads of the same file using separate RecordReaders
ability to split files without scanning for markers
bound the amount of memory needed for reading or writing
metadata stored using Protocol Buffers, which allows addition and removal of fields

#### Hadoop parameters to tune for producion use

Balancing data blocks for a Hadoop cluster
Choosing a proper block size
Using compression for input and output
Setting proper number of map and reduce slots for TaskTracker
Tuning the JobTracker configuration
Tuning the TaskTracker configuration
Tuning shuffle, merge, and sort parameters
Configuring memory for a Hadoop cluster
Setting proper number of parallel copies
Tuning JVM parameters
Configuring JVM Reuse
Configuring the reducer initialization time