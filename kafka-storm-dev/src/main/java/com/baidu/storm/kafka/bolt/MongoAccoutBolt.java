/*
 * authored by lixiao01@baidu.com
 * feature account by IDC every 30s to emit in one Mongo record
 * Date:20130523
 * */

package com.baidu.storm.kafka.bolt;

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
import com.baidu.storm.kafka.common.BaseCounter;
import com.baidu.storm.kafka.common.TupleHelpers;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject; 

import org.json.simple.JSONObject;


public class MongoAccoutBolt<T> extends BaseBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public MongoAccoutBolt(String lostYear, long emitFrequencyInSeconds, String logKey, String logValue) {
		this.lostYear = lostYear;
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		this.logKey = logKey;
		this.logValue = logValue;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("hostname","lognumber", "sumall", "averagesum"));
		declarer.declare(new Fields("records"));
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
			emitCurrentCounts();
		} else {
			countObjAndAck(input);
		}
	}

	private void countObjAndAck(Tuple input) {
		// TODO Auto-generated method stub
		DBObject object = getDBObjectForInput(input);
		if(object.get("Document") != null) {
			String recordValue = null;
			Long counterValue = null;
			String recordKey = null;
			try {
				recordValue = ((BSONObject)object.get("Document")).get(this.logValue).toString();
				counterValue = Long.valueOf(recordValue);
				recordKey = ((BSONObject)object.get("Document")).get(this.logKey).toString();
			} catch (NullPointerException e) {
				recordValue = null;
				counterValue = 0L;
				recordKey = "null";
				e.printStackTrace();
				//System.out.println("######################@@" + recordValue);
			}
			objCounter.incrementCount(recordKey, counterValue);
			//this._collector.ack(input);
		}
	}

	private void emitCurrentCounts() {
		// TODO Auto-generated method stub
		try {
			Map<Object, Long> counts = objCounter.getCounts();
			emit(counts);
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
		//objCounter.wipeArrayList();
		objCounter.wipeObjects();
		//innerRecords.clear();
		//records.clear();
	}
	
	@SuppressWarnings("all")
	private void emit(Map<Object, Long> counts) {
		// TODO Auto-generated method stub
		//ensure to clean
		//innerRecords.clear();
		//records.clear();
		records = new JSONObject();
		
		for(Entry<Object, Long> entry : counts.entrySet()) {
			Object obj = entry.getKey();
			Long countValue = entry.getValue();
			long sumValue = countValue.longValue();
			int arraySize = objCounter.computeObjectSize(obj);
			double avgResponseTime = sumValue / arraySize;
			Integer pvObj = Integer.valueOf(arraySize);
			Double arTimeObj = Double.valueOf(avgResponseTime);
		    //this._collector.emit(new Values(obj, size, count, avgRT));
			/*records should like this 
			 *{JX:{PV:123,AR_TIME:456},TC:{PV:123,AR_TIME:456}...,ST:{PV:XXX,AR_TIME:XXX}}
			*/
			System.out.println("$$$$$$$$$$$$$$@@IDC:" + obj.toString() +
					"%%%%%%%%%%%%%%%%%%@@ pvObj" + pvObj.toString() +
					"&&&&&&&&&&&&&&&&&&@@ arTimeObj"+ arTimeObj.toString());
			synchronized(this) {
				//innerRecords.clear();
				innerRecords = new JSONObject();
			}
			innerRecords.put("PV", pvObj);
			innerRecords.put("AR_TIME", arTimeObj);
			records.put(obj.toString(), innerRecords);
			synchronized(this) {
				innerRecords = null;
			}
		}
		this._collector.emit(new Values(records.toString()));
		synchronized(this) {
			records = null;
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
	private String logKey;
	private String logValue;
	private long emitFrequencyInSeconds;
	private BaseCounter<Object> objCounter;
	private BasicOutputCollector _collector;
	private JSONObject records;
	private JSONObject innerRecords;

}
