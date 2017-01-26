package com.example.sparkstreaming.client;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
public class StreamClient {
		
	public static void main(String[] args) {
		// Check that all required args were passed in.
		if (args.length != 3) {
			System.err.println(
					"Usage: StreamClient <stream-name> <endpoint-url>\n\n" +
							"    <app-name> is the name of the app, used to track the read data in DynamoDB\n" +
							"    <stream-name> is the name of the Kinesis stream\n" +
							"    <endpoint-url> is the endpoint of the Kinesis service\n" +
							"    (e.g. nohup spark-submit --class com.example.sparkstreaming.client.StreamClient --deploy-mode cluster --driver-memory 2G --driver-cores 2 --executor-cores 3 --executor-memory 7G spark-streaming-kinesis-1.0.0-complete.jar Spark-Kinesis-Stream-Client Spark-Streaming https://kinesis.eu-west-1.amazonaws.com &)\n" 
					);
			System.exit(1);
		}

		// Set default log4j logging level to WARN to hide Spark logs
		StreamingExamples.setStreamingLogLevels();

		// Populate the appropriate variables from the given args
		String kinesisAppName = args[0];
		String streamName = args[1];
		String endpointUrl = args[2];

		// Create a Kinesis client in order to determine the number of shards for the given stream
		AmazonKinesisClient kinesisClient =	new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
		kinesisClient.setEndpoint(endpointUrl);

		// Spark Streaming batch interval
		Duration batchInterval = new Duration(2000);

		// Kinesis checkpoint interval.  Same as batchInterval for this example.
		Duration kinesisCheckpointInterval = batchInterval;

		// Get the region name from the endpoint URL to save Kinesis Client Library metadata in
		// DynamoDB of the same region as the Kinesis stream
		String regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName();

		// Setup the Spark config and StreamingContext
		SparkConf sparkConfig = new SparkConf().setAppName(kinesisAppName);
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

		int numStreams = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();
		System.out.println("APP STARTED -------------    Number of Shards :   "+numStreams);
		/* Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards */
		List<JavaDStream<byte[]>> streamsList = new ArrayList<JavaDStream<byte[]>>(numStreams);
		for (int i = 0; i < numStreams; i++) {
			streamsList.add(
					KinesisUtils.createStream(jssc, streamName, endpointUrl, kinesisCheckpointInterval, 
							InitialPositionInStream.LATEST, StorageLevel.MEMORY_AND_DISK_2())
					);
		}

		/* Union all the streams if there is more than 1 stream */
		JavaDStream<byte[]> unionStreams;
		if (streamsList.size() > 1) {
			System.out.println("The Stream has more than One shards");
			unionStreams = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
		} else {
			/* Otherwise, just use the 1 stream */
			System.out.println("The Stream has only One shards");
			unionStreams = streamsList.get(0);
		}
		System.out.println("Transformation  Entry ........  "+unionStreams);
		JavaDStream<String> dStream = unionStreams.map(new Function<byte[], String>() {
			public String call(byte[] line) throws Exception {
				String data =new String(line,StandardCharsets.UTF_8);
				//System.err.println("STDERR File Data Received...      "+data);
				System.out.println("STDOUT File Data Received...      "+data);
				return data;
			}
		});
		/* Output Operation on the DStream Object */
		dStream.print();
		jssc.start();
		jssc.awaitTermination();
	}
}
