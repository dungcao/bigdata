1. Download Spark from https://spark.apache.org/downloads.html
2. pip3 install pyspark
3. pip3 install findspark

Before run +++++++
export SPARK_HOME=/Users/Storage/Soft/spark-3.0.1-bin-hadoop3.2
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3

jupyter notebook


export PATH=/Users/Storage/Soft/scala-2.12.12/bin:$PATH

Start Spark Shell
+ use Scala:  spark-shell
+ use Python: pyspark

Start MongoDB
cd /Users/dungcao/Downloads/mongodb-osx-x86_64-4.0.10/bin (or add to path)
---- sudo to create /data/db folder if not exist
./mongod --dbpath /Users/Storage/MongodbData


DATA: http://api.nobelprize.org/v1/laureate.json

Stop MongoDB
++ from mongo shell
mongo
use admin;
db.shutdownServer()

++ force
ps -ef | grep mongo
kill xxx

++++===================
Hadoop
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

++++===================
Start Spark standardlone
cd /Users/Storage/Soft/spark-3.0.1-bin-hadoop3.2
./sbin/start-all.sh

run a python spark
./bin/spark-submit /Vicohub/BigData/code/spark_streaming.py localhost 9999


with Kafka
./bin/spark-submit --jars ~/Downloads/jar_files/spark-streaming-kafka-0-8_2.11-2.4.3.jar /Vicohub/BigData/Spark_Kafka.py


start server using nc command
nc -l 9999

++++===============
Start Kafka
cd kafka folder
---- uncomment listeners and type localhost in server.properties

start zookeeper  --- bin/zookeeper-server-start.sh config/zookeeper.properties

start kafka ---- bin/kafka-server-start.sh config/server.properties

create topic --- bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic_bigdata

start producer to send message to kafka server ---- bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic_bigdata

consume message --- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic_bigdata --from-beginning

++++=====================
++++=====================
HIVE:
install: https://www.tutorialspoint.com/hive/hive_installation.htm

config HIVE - DERBY
https://cwiki.apache.org/confluence/display/hive/hivederbyservermode

replace guava.xxx in lib of HIVE by guava-jre.xxx in hadoop/share/hdfs/lib


export HADOOP_HOME=/Users/Storage/Soft/hadoop-3.2.1
export HIVE_HOME=/Users/Storage/Soft/apache-hive-3.1.2-bin
export DERBY_HOME=/Users/Storage/Soft/db-derby-10.14.2.0-bin

in derby bin: startNetworkServer

in Hive/bin: run hive --service metastore

init metastore: (in bin) schematool -initSchema -dbType derby

start Hadoop then
hadoop fs -mkdir  /tmp
hadoop fs -mkdir /user/hive/warehouse   # step by step mkdir 
hadoop fs -chmod g+w   /tmp
hadoop fs -chmod g+w   /user/hive/warehouse

replace guava.xxx in lib of HIVE by guava-jre.xxx in hadoop/share/hdfs/lib

start hive in bin


FOR HIVE PYTHON
sudo apt-get install libsasl2-dev sasl thrift thrift-sasl PyHive
