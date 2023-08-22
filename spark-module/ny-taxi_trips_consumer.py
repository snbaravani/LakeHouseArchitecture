from pyspark.sql import SparkSession
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro, to_avro
import pyspark.sql.functions as func

options = {
    "kafka.bootstrap.servers": " PLAINTEXT://kafka:9092",
    "application.id": "lakehouse-app",
    "subscribe": "postgres.public.ny_taxi_trips_data",
    "startingOffsets": "earliest"

}

fromAvroOptions = {"mode": "FAILFAST"}
schema_registry_conf = {'url': "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
value_schema = schema_registry_client.get_latest_version(
    'postgres.public.ny_taxi_trips_data' + "-value").schema.schema_str
print(value_schema)

spark = SparkSession.builder\
.master("local[*]")\
.config("spark.sql.streaming.checkpointLocation","/Users/s115778/Documents/project/medium/lakehouse/checkpoints-18")\
.config("fs.s3a.aws.credentials.provider","com.amazonaws.auth.profile.ProfileCredentialsProvider")\
.config("spark.driver.memory", "15g") \
.config("spark.jars","jars/spark-sql-kafka-0-10_2.12-3.1.1.jar,jars/spark-avro_2.12-3.1.1.jar,jars/common-config-5.2.1.jar,jars/hadoop-aws-3.1.1.jar,"
                      "jars/spark-streaming-kafka_2.10-1.6.3.jar,jars/commons-pool2-2.10.0.jar,jars/aws-java-sdk-bundle-1.11.444.jar,"
                     "jars/kafka-clients-2.3.0.jar,jars/delta-core_2.12-1.0.0.jar,jars/spark-token-provider-kafka-0-10_2.12-3.1.1.jar,"
                      "jars/spark-streaming-kafka-assembly_2.11-1.6.1.jar")\
.appName("lakehouse").getOrCreate()

tripsDfStream = spark.readStream.format("kafka").options(**options).load().withColumn('fixedValue', func.expr("substring(value, 6, length(value)-5)")).select(from_avro('fixedValue', value_schema,       fromAvroOptions).alias('parsedValue'))
tripsDfStream.printSchema()

parsedDF=tripsDfStream.select("parsedValue.after.*")
parsedDF.printSchema()
parsedDF.repartition(150).writeStream.format("delta").outputMode("append") .partitionBy("weekday").option("overwriteSchema", "true") \
    .trigger(processingTime='0 seconds') \
    .start("s3a://nytripslakehouse/nytripslakehousebronze/").awaitTermination()
