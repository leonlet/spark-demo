package com.hapoalim.hapoalimdemo;

import com.hapoalim.hapoalimdemo.kafka.KafkaMessage;
import com.hapoalim.hapoalimdemo.kafka.KafkaProducer;
import com.hapoalim.hapoalimdemo.kafka.KafkaSubscriber;
import com.hapoalim.hapoalimdemo.service.ChangeDataCaptureService;
import com.hapoalim.hapoalimdemo.setup.Constants;
import com.hapoalim.hapoalimdemo.util.ObjectToJsonUtil;

import java.io.IOException;
import java.util.List;


public class HapoalimDemoApplication {

	public static void main(String[] args) throws IOException
	{
		System.out.println("Main Started");
		ObjectToJsonUtil utils = new ObjectToJsonUtil();
		KafkaMessage kafkaMsg;
		String convertedMsg;

		KafkaSubscriber subscriber = new KafkaSubscriber(Constants.DEFAULT_KAFKA_HOST, Constants.INPUT_TOPIC);
		KafkaProducer producer = new KafkaProducer(Constants.DEFAULT_KAFKA_HOST);
		ChangeDataCaptureService captureService = new ChangeDataCaptureService(producer);
		List<String> messages = subscriber.consumeMessages(100L);
		for( String msg : messages)
		{
			convertedMsg = utils.formatKafkaMessage(msg);
			kafkaMsg = utils.convertToKafkaMessage(convertedMsg);
			captureService.process(kafkaMsg);

		}
		try
		{
			subscriber.SparkConsume(captureService);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			System.out.println("Main Done");
		}
	}

}
