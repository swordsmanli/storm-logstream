package com.baidu.storm.kafka.common;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;

public interface IRawMultiScheme extends Serializable {
	public Iterable<List<Object>> deserialize(byte[] ser);
	public Fields getOutputFields();
}
