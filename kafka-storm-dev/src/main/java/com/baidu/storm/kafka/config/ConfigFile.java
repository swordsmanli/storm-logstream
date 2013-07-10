package com.baidu.storm.kafka.config;

public class ConfigFile {
	
	//Storm
	public static Integer STORM_SPOUT_PARALLELISM = 3;
	public static Integer STORM_BOLT_PARALLELISM = 3;
	public static boolean STORM_DEBUG = true;
	
	
	//Kafka
	public static String KFK_SERV_HOSTS = "ip1:port1, ip2:port2 ...";
	public static int KFK_SERV_PORT = 9092;
	public static int KFK_BUFFER_SIZE = 1024*1024;
	public static int KFK_PART_NUMS = 3;
	public static String KFK_GROUP_ID = "";
	public static String KFK_TOPIC = "";
	public static int KFK_CONN_TIMEOUT_MS = 1000;
	
	/*
	 * Kakfa::SimpleConsumer::API
	 * getFirstOffset(topic,partition) send -2
	 * getLastOffset(topic,partition) send -1
	 */
	public static long KFK_START_OFFSET = -1;
	
	//zookeeper
	public static String ZK_ROOT = "/kafka_storm";
	public static String ZK_HOSTS = "ip1,ip2,ipN ...";
	public static int ZK_PORT = 2181;
	public static long ZK_STATE_UPDATE_INTERVAL_MS = 2000;
	
	//PartitionManager
	public static int PM_REFRESH_SECS = 60;
	
}
