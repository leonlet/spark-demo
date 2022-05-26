package com.hapoalim.hapoalimdemo.setup;

public class Constants
{
    public static final String DEFAULT_KAFKA_HOST = "127.0.0.1:9092";
    public static final String DEFAULT_POLLING_TIME = "5";
    public static final String KAFKA = "kafka";
    public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
    public static final String SUBSCRIBER = "subscribe";
    public static final String OUTPUT_TOPIC = "my-cdc-output-topic";
    public static final String INPUT_TOPIC = "my-cdc-input-topic";
    public static final String INSERT_OPERATION = "INSERT";
    public static final String UPDATE_OPERATION = "UPDATE";
    public static final String DELETE_OPERATION = "DELETE";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
}
