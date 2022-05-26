package com.hapoalim.hapoalimdemo.service;

import com.hapoalim.hapoalimdemo.kafka.KafkaMessage;

public interface CaptureService
{
    public void process(KafkaMessage msg);
}
