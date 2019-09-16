package com.joy.zookeeper.sample.watcher;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Repo {
	private static final Logger logger = LoggerFactory.getLogger(Repo.class);
			
	private static Repo instance;
	private Map<String, String> consumerMap = new HashMap<>();
	
	public static Repo getInstance() {
		if (instance == null) {
			instance = new Repo();
		}
		return instance;
	}
	
	public Map<String, String> getRepo() {
		return consumerMap;
	}
	
	public String getRepo(String key) {
		logger.info("consumerMap : " + consumerMap.toString());
		return consumerMap.get(key);
	}
	
	public void addRepo(String key, String value) {
		consumerMap.put(key, value);
	}
}
