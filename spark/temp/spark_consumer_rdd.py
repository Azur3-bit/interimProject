from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Initialize SparkContext and StreamingContext
sc = SparkContext(appName="KafkaRDDConsumer")
ssc = StreamingContext(sc, 5)  # 5 seconds batch interval

# Create Direct Kafka Stream
kafkaStream = KafkaUtils.createDirectStream(ssc, ['server-logs'], {"metadata.broker.list": 'localhost:9092'})

# Extract data from Kafka Stream
kafkaRDD = kafkaStream.map(lambda message: message[1])

# Perform operations on RDD
# Example: Count occurrences of each log level
logLevelsRDD = kafkaRDD.map(lambda log: (log.split(" - ")[1], 1))
logLevelCounts = logLevelsRDD.reduceByKey(lambda x, y: x + y)

# Print counts to console
logLevelCounts.pprint()

# Start the streaming context and await termination
ssc.start()
ssc.awaitTermination()
