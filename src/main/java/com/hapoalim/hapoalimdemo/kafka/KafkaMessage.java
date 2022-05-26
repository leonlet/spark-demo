package com.hapoalim.hapoalimdemo.kafka;

import java.io.Serializable;
import java.util.Map;

public class KafkaMessage implements Serializable
{
    private String pk;

   public KafkaMessage()
   {

   }
    @Override
    public String toString()
    {
        return "KafkaMessage{" +
                "pk='" + pk + '\'' +
                ", data=" + data +
                ", beforeData='" + beforeData + '\'' +
                ", headers=" + headers +
                '}';
    }

    private Map<String,String> data;
    private String beforeData;
    private Map<String, String> headers;

    public String getPk()
    {
        return pk;
    }

    public void setPk(String pk)
    {
        this.pk = pk;
    }

    public Map <String,String> getData()
    {
        return data;
    }

    public void setData(Map<String,String> data)
    {
        this.data = data;
    }

    public String getBeforeData()
    {
        return beforeData;
    }

    public void setBeforeData(String beforeData)
    {
        this.beforeData = beforeData;
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    public void setHeaders(Map<String, String> headers)
    {
        this.headers = headers;
    }

}
