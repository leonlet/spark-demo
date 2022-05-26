package com.hapoalim.hapoalimdemo.service;
import com.hapoalim.hapoalimdemo.kafka.KafkaMessage;
import com.hapoalim.hapoalimdemo.kafka.KafkaProducer;
import com.hapoalim.hapoalimdemo.setup.Constants;
import com.hapoalim.hapoalimdemo.util.ObjectToJsonUtil;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ChangeDataCaptureService implements CaptureService , Serializable
{
    public ChangeDataCaptureService(KafkaProducer producer)
    {

    }


    @Override
    public void process(KafkaMessage msg)
    {
        System.out.println("capture method invoked");
        if(msg==null)
        {
           throw new RuntimeException();
        }
        Map<String, String> headers = msg.getHeaders();
        ObjectToJsonUtil util = new ObjectToJsonUtil();
        String operation =  headers.get("operation");
        String pk = msg.getPk();
        String outputMsg;
        KafkaProducer producer = new KafkaProducer(Constants.DEFAULT_KAFKA_HOST);
        if(operation==null)
        {
           System.out.println("Mandatory Operation field missing");
           throw new RuntimeException();
        }
        if(isUpdateInsertOperation(operation))
           outputMsg = util.convertUpdateOutputToJson(msg);
        else if(isDeleteOperation(operation))
            outputMsg = util.convertDeleteOutputToJson(msg);
        else
        {
           System.out.println("Illegal operation provided ");
           throw new RuntimeException();
        }
        try
        {
            producer.publish(Constants.OUTPUT_TOPIC,outputMsg,pk);
        }
        catch (IOException |ExecutionException |InterruptedException |TimeoutException e)
        {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    private boolean isDeleteOperation(String operation)
    {
        return operation.equalsIgnoreCase(Constants.DELETE_OPERATION);
    }

    private boolean isUpdateInsertOperation(String operation)
    {
        return operation.equalsIgnoreCase(Constants.INSERT_OPERATION)
                ||operation.equalsIgnoreCase(Constants.UPDATE_OPERATION);
    }

}
