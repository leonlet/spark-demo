package com.hapoalim.hapoalimdemo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hapoalim.hapoalimdemo.kafka.KafkaMessage;
import com.hapoalim.hapoalimdemo.service.CaptureService;
import com.hapoalim.hapoalimdemo.setup.Constants;
import com.hapoalim.hapoalimdemo.util.ObjectToJsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
class HapoalimDemoApplicationTests
{

public static final String INSERT_INPUT ="src/main/resources/KafkaInputInsert.json";
public static final String DELETE_INPUT = "src/main/resources/KafkaInputDelete.json";

	@Test
	void testConvertInsertOperationToJson()
	{
		ObjectToJsonUtil util = new ObjectToJsonUtil();
		KafkaMessage msg = new KafkaMessage();
		Map map = new HashMap<String,String>();
		map.put("a","foo");
		map.put("b","bar");
		msg.setPk("123456");
		msg.setData(map);
		String insertJson = util.convertUpdateOutputToJson(msg);
		System.out.println(insertJson);
		Assertions.assertNotNull(insertJson);

	}
    @Test
	void testConvertDeleteOperationToJson()
	{
		ObjectToJsonUtil util = new ObjectToJsonUtil();
		KafkaMessage msg = new KafkaMessage();
		Map map = new HashMap<String,String>();
		Map headers = new HashMap<String,String>();
		headers.put("operation","DELETE");
		map.put("a","foo");
		map.put("b","bar");
		msg.setPk("123456");
		msg.setData(map);
		msg.setHeaders(headers);
		String deleteJson = util.convertDeleteOutputToJson(msg);
		Assertions.assertNotNull(deleteJson);
		System.out.println(deleteJson);

	}
    @Test
	public void testEmptyConsumeMessage()
	{
		KafkaMessage msg = new KafkaMessage();
		CaptureService service = mock(CaptureService.class);
		doThrow(new RuntimeException()).when(service).process(isNull());
		assertThrows(RuntimeException.class,()-> service.process(null));

	}
	@Test
	public void testInsertParseMsg() throws IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		KafkaMessage obj = mapper.readValue(new File(INSERT_INPUT), KafkaMessage.class);
		Assertions.assertNotNull(obj);
		Assertions.assertNotNull(obj.getHeaders());
		String actualOpr = obj.getHeaders().get("operation");
		assertEquals(Constants.INSERT_OPERATION,actualOpr);
		System.out.println(obj.toString());
	}
	@Test
	public void testDeleteParseMsg() throws IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		KafkaMessage obj = mapper.readValue(new File(DELETE_INPUT), KafkaMessage.class);
		Assertions.assertNotNull(obj);
		Assertions.assertNotNull(obj.getHeaders());
		String actualOpr = obj.getHeaders().get("operation");
		assertEquals(Constants.DELETE_OPERATION,actualOpr);
		System.out.println(obj.toString());
	}

}
