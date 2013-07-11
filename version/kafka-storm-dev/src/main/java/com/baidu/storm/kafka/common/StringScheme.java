package com.baidu.storm.kafka.common;

import java.io.UnsupportedEncodingException;
import java.util.List;

import static java.util.Arrays.asList;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import static backtype.storm.utils.Utils.tuple;

public class StringScheme implements IRawMultiScheme{

	@Override
	public Iterable<List<Object>> deserialize(byte[] ser) {
		// TODO Auto-generated method stub
		return asList(tuple(ser));
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("bytes");
	}

}
