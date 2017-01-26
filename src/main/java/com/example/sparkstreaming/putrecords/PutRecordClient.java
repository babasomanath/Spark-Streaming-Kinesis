/**
 * This is meant for putting records into the Stream 
 */
package com.example.sparkstreaming.putrecords;

import java.nio.ByteBuffer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

/**
 * @author somanatn
 *
 */
public class PutRecordClient {
	private static final String REGION = "eu-west-1";
	private static final String STREAM_NAME = "Spark-Streaming";
	public static void main(String[] args) {
		try{
			AWSCredentials credentials  = new ProfileCredentialsProvider().getCredentials();
			AmazonKinesisClient kinesisClient = new AmazonKinesisClient(credentials).withEndpoint("https://kinesis.eu-west-1.amazonaws.com").
					withRegion(Regions.fromName(REGION));
			//String sequenceNumberOfPreviousRecord = "99999999999999999999999999999990";
			//String sequenceNumberOfPreviousRecord = "0";
			for (int j = 0; j < 9999999; j++) 
			{
				Long nanoTime = System.nanoTime();
				String partitionKey = nanoTime.toString();
				PutRecordRequest putRecordRequest = new PutRecordRequest();
				putRecordRequest.setStreamName(STREAM_NAME);
				putRecordRequest.setData(ByteBuffer.wrap( String.format( "Time : "+partitionKey+"   BOOTCAMP === HELLO ### CLOUD====BABA ==>  %d", j ).getBytes() ));
				putRecordRequest.setPartitionKey( partitionKey);  
				//putRecordRequest.setSequenceNumberForOrdering( sequenceNumberOfPreviousRecord );
				PutRecordResult putRecordResult = kinesisClient.putRecord( putRecordRequest );
				//sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();
				System.out.println(putRecordResult);
				Thread.sleep(100);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

}
