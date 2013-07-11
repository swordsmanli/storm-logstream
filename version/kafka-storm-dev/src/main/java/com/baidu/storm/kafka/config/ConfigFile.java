package com.baidu.storm.kafka.config;

public class ConfigFile {
	
	//Storm
	public static Integer STORM_SPOUT_PARALLELISM = 1;
	public static Integer STORM_BOLT_PARALLELISM = 1;
	public static boolean STORM_DEBUG = true;
	
	
	//Kafka
	public static String KFK_SERV_HOSTS = "10.65.45.98:9092, 10.65.45.98:20000";
	public static int KFK_SERV_PORT = 9092;
	public static int KFK_BUFFER_SIZE = 1024*1024;
	public static int KFK_PART_NUMS = 2;
	public static String KFK_GROUP_ID = "imas_log";
	public static String KFK_TOPIC = "imas_e";
	public static int KFK_CONN_TIMEOUT_MS = 1000;
	
	/*
	 * Kakfa::SimpleConsumer::API
	 * getFirstOffset(topic,partition) send -2
	 * getLastOffset(topic,partition) send -1
	 */
	public static long KFK_START_OFFSET = -1;
	
	//Zookeeper
	public static String ZK_ROOT = "/kafka_storm";
	public static String ZK_HOSTS = "10.40.44.34,10.40.42.42,10.40.43.48";
	public static int ZK_PORT = 2181;
	public static long ZK_STATE_UPDATE_INTERVAL_MS = 2000;
	
	//PartitionManager
	public static int PM_REFRESH_SECS = 60;
	
	//Mongodb
	public static String MONGO_HOST = "10.65.43.129";
	public static int MONGO_PORT = 8500;
	public static String MONGO_DB_NAME = "logstream";
	public static String MONGO_COLL_NAME = "test";
	
	//LOG ACCOUNT
	public static String LOG_LOST_YEAR = "2013";
	public static String LOG_ACCOUNT_KEY = "idc";
	public static String LOG_ACCOUNT_VALUE = "tt";
	public static long LOG_EMIT_INTERVAL_MS = 60;
	
	
	
	
	
	
}
