package com.hapoalim.hapoalimdemo.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hapoalim.hapoalimdemo.service.CaptureService;
import com.hapoalim.hapoalimdemo.service.ChangeDataCaptureService;
import com.hapoalim.hapoalimdemo.util.ObjectToJsonUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaSubscriber implements Serializable
{

    private final Consumer<String, String> consumer;
    private static int groupIdCount = 1;
    private CaptureService service;

    public KafkaSubscriber(String bootstrapServer, String topicName)
    {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG , "group" + groupIdCount++);
        prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG ,"hapoalim_client");
        this.consumer = new KafkaConsumer<>(prop);
        this.consumer.subscribe(Collections.singletonList(topicName));

    }
    public List<String> consumeMessages(long duration)
    {
        System.out.println("Request for kafka message count for duration: " +duration +" milliseconds");

        List<String> finalResult = new ArrayList<>();
        try
        {
            ConsumerRecords<String, String> records = this.consumer.poll(duration);
            for (ConsumerRecord<String, String> record : records)
            {
                finalResult.add(record.value());
            }
            for (TopicPartition tp : consumer.assignment())
                System.out.println("Committing offset at position:" + consumer.position(tp));
            consumer.commitSync();
            System.out.println("Fetched " + finalResult.size() + " messages from kafka");
        }
        catch (Exception exe)
        {
            exe.printStackTrace();
            throw exe;
        }
        finally
        {
            consumer.close();
            System.out.println("Closed consumer");
        }

        return finalResult;
    }

    public void SparkConsume(ChangeDataCaptureService service) throws InterruptedException, JsonProcessingException
    {
        this.service= service;
        ObjectToJsonUtil utils = new ObjectToJsonUtil();
        System.setProperty("hadoop.home.dir", "c://hadoop//winutil//");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("WordCountingApp");
        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
        sparkConf.set("spark.master","local");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("my-cdc-input-topic");
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
        JavaDStream<String> meetupStreamValues = messages.map(v -> {
            return v.value();

        });
        JavaDStream<KafkaMessage> kafkaData = meetupStreamValues.map(e -> {
            ObjectMapper mapper = new ObjectMapper();
            KafkaMessage obj = mapper.readValue(e, KafkaMessage.class);
            KafkaMessage newObj = new KafkaMessage();
            newObj.setData(obj.getData());
            service.process(obj);
            return obj;
        });
        kafkaData.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
