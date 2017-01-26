# Objective 1:
   Create a Spark Streaming Java Client to read the data from the Amazon Kinesis Stream and print them in the stdout file. Run this application on an EMR cluster. Monitor the std out logs of the containers as shown in the diagram as well as the dynamoDB table to confirm the checkpoinitng.
     
     ---------------------
     Submission of job : 
     ---------------------
     nohup spark-submit --class com.example.sparkstreaming.client.StreamClient --deploy-mode cluster 
     --driver-memory 2G --driver-cores 2--executor-cores 3 --executor-memory 7G 
     spark-streaming-kinesis-1.0.0-complete.jar Spark-Kinesis-Stream-Client Spark-Streaming https://kinesis.eu-west-1.amazonaws.com &

## EMR Monitoring Diagrams:
![alt tag](https://github.com/babasomanath/Spark-Streaming-Kinesis/blob/master/files/Validate.png)

## DynamoDB Check Pointing Diagrams:
![alt tag](https://github.com/babasomanath/Spark-Streaming-Kinesis/blob/master/files/DDB_checkpoint.png)

## Monitoring the SparkStages from Spark History Server UI
![alt tag](https://github.com/babasomanath/Spark-Streaming-Kinesis/blob/master/files/Monitoring_The_SparkStages_From_Spark_History_Server_UI.png)

# Producer Sample Code:
https://github.com/babasomanath/Spark-Streaming-Kinesis/blob/master/src/main/java/com/example/sparkstreaming/putrecords/PutRecordClient.java
   
