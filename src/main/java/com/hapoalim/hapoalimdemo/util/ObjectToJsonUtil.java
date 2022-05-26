package com.hapoalim.hapoalimdemo.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hapoalim.hapoalimdemo.kafka.KafkaMessage;

import java.io.IOException;
import java.util.Map;

public class ObjectToJsonUtil
{
    public KafkaMessage convertToKafkaMessage(String kafkaMsg) throws JsonProcessingException
    {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(kafkaMsg, KafkaMessage.class);
    }
    public String convertUpdateOutputToJson(KafkaMessage msg)
    {
        ObjectMapper Obj = new ObjectMapper();
        String jsonStr = null;
        try {

            jsonStr = Obj.writeValueAsString(msg.getData());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return jsonStr;
    }
    public String convertDeleteOutputToJson(KafkaMessage msg)
    {
        ObjectMapper Obj = new ObjectMapper();
        String jsonStr = null;
        try {
            Map<String, String> headers = msg.getHeaders();
           // String streamPosition = (String)headers.get("streamPosition");
            jsonStr = Obj.writeValueAsString(null);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return jsonStr;
    }
    public String formatKafkaMessage(String msg)
    {
        return (msg == null ? null : msg.replace("\n", "")
                .replace("\r", "")
                .replace("[","")
                .replace("]","")
                .replace("'",""));

    }

}

