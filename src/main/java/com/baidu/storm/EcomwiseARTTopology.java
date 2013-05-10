package com.baidu.storm;

import static backtype.storm.utils.Utils.tuple;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import com.baidu.storm.common.MongoToTupleFormatter;
import com.baidu.storm.bolt.MongoAccoutBolt;
import com.baidu.storm.bolt.MongoInsertBolt;
import com.baidu.storm.spout.MongoSpout;

class InsertHelper implements Runnable {

	public InsertHelper(String dbHost, int dbPort, String dbName, 
			String collectionName, CountDownLatch latch) {
		this.dbHost = dbHost;
		this.dbPort = dbPort;
		this.dbName = dbName;
		this.collectionName = collectionName;
		this.latch = latch;
	}
		
	@Override
	public void run() {
		// TODO Auto-generated method stub
		DBCollection coll;
		try {
			coll = DBHelperMethod.getDBCollection(this.dbHost, this.dbPort, 
					this.dbName, this.collectionName);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//wait until storm main thread ready
		while(this.latch.getCount() != 0) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//insert something as you want	
	}

	private String dbHost;
	private int dbPort;
	private String dbName;
	private String collectionName;
	private CountDownLatch latch;
	
}

class DBHelperMethod {
	public static DBCollection getDBCollection(String mongHost, int mongoPort, 
			String dbName, String collectionName) throws UnknownHostException, MongoException {
		DBCollection collection;
		Mongo mongo = new Mongo(mongHost, mongoPort);
		DB db = mongo.getDB(dbName);
		collection = db.getCollection(collectionName);
		return collection; 
	}
	
	public static DBCollection createCappedDBCollection(String mongHost, int mongoPort, 
			String dbName, String collectionName) throws UnknownHostException, MongoException {
		DBCollection collection;
		Mongo mongo = new Mongo(mongHost, mongoPort);
		mongo.dropDatabase(dbName);
		DB db = mongo.getDB(dbName);
		collection = db.createCollection(collectionName, 
				new BasicDBObject("capped", true).append("size", 10000000));
		
		return collection;
	}
}

public class EcomwiseARTTopology {

	/**
	 * @param args
	 * @throws MongoException 
	 * @throws UnknownHostException 
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws InterruptedException 
	 */
	
	public static void main(String[] args) throws UnknownHostException, AlreadyAliveException, InvalidTopologyException, InterruptedException {
		// TODO Auto-generated method stub
		final String mongoHost ="10.65.45.107"; //"10.65.43.129";"10.65.45.107";
		final int mongoPort = 8500;
		final String spoutDbName = "ecomwise";//"online";
		final String spoutCollectionName = "wisebusiness";//"imas";
		final String boltDbName = "wise";//"online";
		final String boltCollectionName = "ecomwise_timeout";
		final String lostYear = "2013";
		long intervalTime = 30;
		Date startDate = new Date(System.currentTimeMillis());
		
		//main thread to await all the other threads
		CountDownLatch latch = new CountDownLatch(1);
		InsertHelper inserter = new InsertHelper(mongoHost, mongoPort, boltDbName, 
				boltCollectionName, latch);
		new Thread(inserter).start();
		
		//get a bolt mongdb collection
		final DBCollection coll = DBHelperMethod.createCappedDBCollection(mongoHost, mongoPort, 
				boltDbName, boltCollectionName);
		
				
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * wired java function
		 * Anonymous Inner Class initialization
		 * must override unimplemented functions
		 */
		MongoSpout spout = new MongoSpout(mongoHost, mongoPort, spoutDbName, 
				spoutCollectionName, new BasicDBObject()) {


			private static final long serialVersionUID = 1L;
			
			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				// TODO Auto-generated method stub
				declarer.declare(new Fields("Document"));
			}

			@Override
			public List<Object> dbObjectToStormTuple(DBObject message) {
				// TODO Auto-generated method stub
				return tuple(message);
			}

			@Override
			public void activate() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void deactivate() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public Map<String, Object> getComponentConfiguration() {
				// TODO Auto-generated method stub
				return null;
			}
			
		};	
		
		MongoToTupleFormatter formatter = new MongoToTupleFormatter() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public DBObject mapTupleToDBObject(DBObject object, Tuple tuple) {
				//do construction of mongodb object
				return BasicDBObjectBuilder.start()
						.add("hostname", tuple.getStringByField("hostname"))
						.add("wise_ar_time", tuple.getDoubleByField("averagesum"))
						.add("log_number", tuple.getIntegerByField("lognumber"))
						.add("all_sum", tuple.getLongByField("sumall"))
						.add("timestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
						.get();
			}			
		};
		
		MongoInsertBolt mongoInserter = new MongoInsertBolt("mongodb://" + mongoHost 
				+ ":" + mongoPort + "/" + boltDbName, boltCollectionName, formatter, 
				WriteConcern.NONE, true);
		
		//add spout and bolts
		builder.setSpout("imaslog", spout, 1);
		builder.setBolt("sum", new MongoAccoutBolt<Object>(lostYear, intervalTime), 1)
			.allGrouping("imaslog");
		builder.setBolt("mongoInserter", mongoInserter, 1).allGrouping("sum");
		
		//set debug config
		Config conf = new Config();
		conf.setDebug(true);
		
		if(args != null && args.length > 0) {
			conf.setNumAckers(3);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			// Run on local cluster
			LocalCluster cluster = new LocalCluster();
			
			//submit the topology
			cluster.submitTopology("mongoStorm", conf, builder.createTopology());
			
			//starting insertion
			latch.countDown();
			
			//Thread.sleep(10000);
			//cluster.shutdown();
		}
		
	}

}
