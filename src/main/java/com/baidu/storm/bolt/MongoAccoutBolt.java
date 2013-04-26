package com.baidu.storm.bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.BSONObject;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.topology.base.BaseBasicBolt;
import com.baidu.storm.common.BaseCounter;
import com.baidu.storm.common.TupleHelpers;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject; 


public class MongoAccoutBolt<T> extends BaseBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public MongoAccoutBolt(String lostYear, long emitFrequencyInSeconds) {
		this.lostYear = lostYear;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds; 
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("hostname","lognumber", "sumall", "averagesum"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		objCounter = new BaseCounter<Object>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		this._collector = collector;
		if (TupleHelpers.isTickTuple(input)) {
			System.out.println("##########################" +
					"#############################" +
					"#######################");
			emitCurrentCounts();
		} else {
			
			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" +
					"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" +
					"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			countObjAndAck(input);
		}
	}

	private void countObjAndAck(Tuple input) {
		// TODO Auto-generated method stub
		DBObject object = getDBObjectForInput(input);
		if(object.get("imasDocument") != null) {
			String tt = ((BSONObject)object.get("imasDocument")).get("tt").toString();
			Long imas_time = Long.valueOf(tt);
			String hostname = ((BSONObject)object.get("imasDocument")).get("hostname").toString();
			objCounter.incrementCount(hostname, imas_time);
			//this._collector.ack(input);
		}
	}

	private void emitCurrentCounts() {
		// TODO Auto-generated method stub
		Map<Object, Long> counts = objCounter.getCounts();
		emit(counts);
		//objCounter.wipeArrayList();
		objCounter.wipeObjects();
	}

	private void emit(Map<Object, Long> counts) {
		// TODO Auto-generated method stub
		for(Entry<Object, Long> entry : counts.entrySet()) {
			Object obj = entry.getKey();
			Long count = entry.getValue();
			long countNum = count.longValue();
			int arraySize = objCounter.computeObjectSize(obj);
			double avgImasResponseTime = countNum / arraySize;
			Integer size = Integer.valueOf(arraySize);
			Double avgRT = Double.valueOf(avgImasResponseTime);
		    this._collector.emit(new Values(obj, size, count, avgRT));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}
	
	public DBObject getDBObjectForInput(Tuple input) {
		BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();
		for (String field : input.getFields()) {
			 Object value = input.getValueByField(field);
			 if (isValidDBObjectField(value)) {
				 dbObjectBuilder.append(field, value);
			 }
		}
		return dbObjectBuilder.get();
	}
	
	private boolean isValidDBObjectField(Object value) {
		return value instanceof String
				|| value instanceof Date
				|| value instanceof Integer
				|| value instanceof Float
				|| value instanceof Double
				|| value instanceof Short
				|| value instanceof Long
				|| value instanceof DBObject;
	}
	
	private String lostYear;
	private long emitFrequencyInSeconds;
	private BaseCounter<Object> objCounter;
	private BasicOutputCollector _collector;

}
