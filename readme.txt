1. Download Spark from https://spark.apache.org/downloads.html
2. pip3 install pyspark
3. pip3 install findspark

Before run +++++++
export SPARK_HOME=/Soft/spark-2.4.3-bin-hadoop2.7/
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3

jupyter notebook


export PATH=/Soft/scala-2.13.0/bin:$PATH

Start Spark Shell
+ use Scala:  spark-shell
+ use Python: pyspark

Start MongoDB
cd /Users/dungcao/Downloads/mongodb-osx-x86_64-4.0.10/bin (or add to path)
---- sudo to create /data/db folder if not exist
./mongod --dbpath /MongodbData


Stop MongoDB
++ from mongo shell
mongo
use admin;
db.shutdownServer()

++ force
ps -ef | grep mongo
kill xxx

++++===================
Start Spark standardlone
cd /Soft/spark-2.4.3-bin-hadoop2.7/
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
