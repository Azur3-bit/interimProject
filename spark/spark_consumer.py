from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Initialize SparkContext and StreamingContext
sc = SparkContext(appName="KafkaLogProcessing")
ssc = StreamingContext(sc, 5)

# Create Kafka stream
kafka_stream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'log_topic': 1})

# Extract messages
lines = kafka_stream.map(lambda x: x[1])

# Transformation 1: Filter ERROR messages
error_lines = lines.filter(lambda line: 'ERROR' in line)

# Transformation 2: Count total log messages
log_count = lines.count()

# Transformation 3: Count ERROR messages
error_count = error_lines.count()

# Print counts
log_count.pprint()
error_count.pprint()

# Start streaming
ssc.start()
ssc.awaitTermination()
