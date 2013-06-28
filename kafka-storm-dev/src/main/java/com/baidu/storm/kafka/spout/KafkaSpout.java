package com.baidu.storm.kafka.spout;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class KafkaSpout extends BaseRichSpout {

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		Map stateConf = new HashMap(conf);
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	
	private SpoutOutputCollector _collector;
}
