from pyspark import SparkContext
sc = SparkContext("local", "weblog mining app")
rdd1 = sc.textFile('hdfs://localhost:9000/wedlog.csv')

counts = rdd1.filter(lambda s: s.split(',')[2].startswith("GET")).map(lambda s: (s.split(',')[1].split(':')[0][1:],1))

print(counts.countByKey())
