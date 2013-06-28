package com.baidu.storm.kafka.common;

import java.io.Serializable;

import com.baidu.storm.kafka.config.ConfigFile;

public class KafkaHostPort implements Serializable {

	/**
	 * data structure to store kafka broker ip with port 
	 */
	public KafkaHostPort(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public KafkaHostPort(String host) {
		this.host = host;
		this.port = ConfigFile.KFK_SERV_PORT;
	}
	
	private static final long serialVersionUID = 1L;
	private String host;
	private int port;

}
