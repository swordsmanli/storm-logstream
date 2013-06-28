package com.baidu.storm.kafka.common;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StringScheme implements Scheme{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public List<Object> deserialize(byte[] ser) {
		// TODO Auto-generated method stub
		try{
			return new Values(new String(ser, "UTF-8"));
		}catch(UnsupportedEncodingException e){
			throw new RuntimeException(e);
		}
	}

	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("data");
	}

}
