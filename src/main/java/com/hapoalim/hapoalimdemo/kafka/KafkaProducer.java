package com.hapoalim.hapoalimdemo.kafka;

import com.hapoalim.hapoalimdemo.setup.Constants;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hapoalim.hapoalimdemo.setup.Constants.BOOTSTRAP_SERVER;
import static com.hapoalim.hapoalimdemo.setup.Constants.KEY_SERIALIZER;
import static com.hapoalim.hapoalimdemo.setup.Constants.VALUE_SERIALIZER;

public class KafkaProducer implements Serializable
{

    private static final String DEFAULT_KEY = "key1";
    private  final Producer<String, String> producer;


    public KafkaProducer(String bootstrapServer) {

        Properties prop = new Properties();
        System.out.println("Creating Kafka Producer...");
        System.out.println("Connecting to Kafka " + bootstrapServer);
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer(prop);
    }

    public void publish(String topic, String msg, String key) throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        if (key == null)
        {
            key = "key1";
        }
        System.out.println(new Object[]{msg, topic, key});
         ProducerRecord<String, String> record = new ProducerRecord(topic, key, msg);
        Future result = this.producer.send(record);

        try
        {
            RecordMetadata metadata = (RecordMetadata) result.get(10L, TimeUnit.SECONDS);
            System.out.println("Successfully  sent message to partition " + metadata.partition()
                    + "on topic " + metadata.topic()
                    + "Message Offset is " + metadata.offset());
        }
        catch (ExecutionException | InterruptedException | TimeoutException exe)
        {
            exe.printStackTrace();
            throw exe;
        }
        finally
        {
            producer.close();
        }
    }
    public void cleanUpKafkaProducerSetup() {
        this.producer.close();
    }
}