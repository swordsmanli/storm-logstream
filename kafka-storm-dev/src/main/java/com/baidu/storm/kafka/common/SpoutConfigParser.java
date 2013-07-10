package com.baidu.storm.kafka.common;

import java.io.Serializable;
import java.util.List;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;

import com.baidu.storm.kafka.common.KafkaHostPort;
import com.baidu.storm.kafka.config.ConfigFile;
import com.baidu.storm.kafka.common.CommonUtils;


public class SpoutConfigParser implements Serializable {

	/**
	 * 
	 */
	
	public SpoutConfigParser() {}
	
	public void forceStartOffset(long millis) {
		this.startOffset = millis;
		if(-1 == this.startOffset) {
			forceFromStart = true;
		}
	}
	
	private static final long serialVersionUID = 1L;
	
	//read zookeeper config parameters
	public List<String> zkServers = CommonUtils
			.getStaticHosts(ConfigFile.ZK_HOSTS);
	public Integer zkPort = ConfigFile.ZK_PORT;
	public String zkRoot = ConfigFile.ZK_ROOT;
	public long stateUpdateIntervalsMs = ConfigFile.ZK_STATE_UPDATE_INTERVAL_MS;
	
	//read kafka config parameters
	public List<String> kafkaHosts = CommonUtils
			.getStaticHosts(ConfigFile.KFK_SERV_HOSTS);
	public List<KafkaHostPort> kafkaBrokerPairs = CommonUtils
			.convertHosts(kafkaHosts);
	public int kafkaPort = ConfigFile.KFK_SERV_PORT;
	public int partitionsPerHost = ConfigFile.KFK_PART_NUMS;
	public int fetchSizeBytes = ConfigFile.KFK_BUFFER_SIZE;
	public int bufferSizeBytes = ConfigFile.KFK_BUFFER_SIZE;
	public int socketTimeoutMs = ConfigFile.KFK_CONN_TIMEOUT_MS;
	public String kafkaTopic = ConfigFile.KFK_TOPIC;
	public String kafkaGroupId = ConfigFile.KFK_GROUP_ID;
	
	public int pm_refresh_secs = ConfigFile.PM_REFRESH_SECS;
	
	public long startOffset = -1;
	public boolean forceFromStart = false;
	
	public IRawMultiScheme scheme = new StringScheme();
	//public Scheme scheme = new StringScheme();
}
