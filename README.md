# LakeHouseArchitecture
https://medium.com/@snbaravani/real-time-party-of-data-streaming-analytics-and-visualisation-in-the-lake-house-951d4fa96afb

Running the application

Install Docker, Python3 and Pyspark etc . You would figure out the missing packages if any while running the spark job
Clone the repo from here https://github.com/snbaravani/LakeHouseArchitecture.git
Change the volume path in the postgres section in the docker-compose.yaml. Run the docker compose docker-compose up and doker ps should show you the following containers.

4. Copy the data in the csv file to postgres after copying the csv to docker

docker cp /Users/../../NY_Taxi_Rich.csv docker_code_postgres_1:scripts

COPY ny_taxi_trips_data
FROM '/scripts/NY_Taxi_Rich.csv'
DELIMITER ',' 
CSV HEADER;
You should see ~ 2 million records in the the table.


5. Run the debezium connector with the json payload at 127.0.0.1:8083/connectors/ and and it will immediately move all the records in the postgres table to kafka topic “postgres.public.ny_taxi_trips_data” (auto created). Any update or insertion to the table will be streamed into the topic from now on. Run the Kafkacat command to see the data in the topic

kafkacat -C -b localhost:9092 \
    -t postgres.public.ny_taxi_trips_data \
    -r http://localhost:8081 \
   -o -1 -s value=avro  
    -e -q \
    | grep -v "Reading configuration from file" | wc -l

7. Create an s3 bucket and give access to Athena, Glue and QuickSight. Configure AWS credentials on your machine.

8. Run the spark job ny-taxi_trips_consumer.py after changing the bucket URN in the code and within few seconds parquet files will be generated in the s3 bucket partitioned by weekdays


9. Create a Glue crawler and point it to the bucket created above and run the crawler. More details here

10. You will see the data (~2 million) in the Athena table nytripslakehouse.


11. QuickSight visuals need to be created manually and that that is out of scope for this article :-)
