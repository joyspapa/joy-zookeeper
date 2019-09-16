package com.joy.zookeeper.sample;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperHandlerTest {
	private static final Logger logger = LoggerFactory.getLogger(ZooKeeperHandlerTest.class);

	private static ZooKeeperHandlerTest instance;
	private ZooKeeperConnectionTest zooKeeperConnection;
	private ZooKeeper zk;
	private String zkHost;

	private final Charset ENCODING = StandardCharsets.UTF_8;

	public static ZooKeeperHandlerTest getInstance(String zkHost) throws Exception {
		if (instance == null) {
			instance = new ZooKeeperHandlerTest(zkHost);
		}
		return instance;
	}

	public ZooKeeperHandlerTest(String zkHost) throws Exception {

		if (zkHost == null || zkHost.isEmpty()) {
			throw new IllegalArgumentException(
					"[ZooKeeperAdapterAbstract(String zkHost)] zkHost is NULL. zkHost=" + zkHost);
		}
		this.zkHost = zkHost;
		zooKeeperConnection = new ZooKeeperConnectionTest(zkHost);
		zk = zooKeeperConnection.getConnect();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			closeConnection();
		}));
	}

	public void closeConnection() {
		zooKeeperConnection.close();
		logger.info("zookeeper connection is closed!");
	}

	public void create(String path, byte[] data) throws Exception {
		create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public void create(String path, byte[] data, CreateMode createMode) throws Exception {
		create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
	}

	/*
	 * zk - ZooKeeper 
	 * path - the path for the node
	 * data - the initial data for the node
	 * acl - the acl for the node
	 * createMode – specifying whether the node to be created is ephemeral and/or sequential
	 */
	public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws Exception {
		if (exists(path) == null) {
			zk.create(path, data, acl, createMode);
		} else {
			setData(path, data);
		}
	}

	public Stat exists(String path) throws Exception {
		return exists(path, false);
	}

	public Stat exists(String path, boolean watchFlag) throws Exception {
		try {
			return zk.exists(path, watchFlag);
		} catch (KeeperException.ConnectionLossException ex) {
			zooKeeperConnection.retryConnect(zkHost);
			return zk.exists(path, watchFlag);
		}
	}

	public byte[] getData(String path) throws Exception {
		return getData(path, false);
	}

	public byte[] getData(String path, boolean watchFlag) throws Exception {
		if (exists(path, watchFlag) != null) {
			return zk.getData(path, watchFlag, null);
		}
		return null;
	}

	public String getDataString(String path) throws Exception, UnsupportedEncodingException {
		return getDataString(path, false, ENCODING.displayName());
	}

	public String getDataString(String path, String encoding) throws Exception, UnsupportedEncodingException {
		return getDataString(path, false, encoding);
	}

	public String getDataString(String path, boolean watchFlag, String encoding)
			throws Exception, UnsupportedEncodingException {
		byte[] binData = getData(path, watchFlag);
		if (binData != null) {
			return new String(binData, encoding);
		}
		return null;
	}

	public void setData(String path, byte[] data) throws Exception {
		setData(path, data, false);
	}

	public void setData(String path, byte[] data, boolean watchFlag) throws Exception {
		Stat stat = exists(path, watchFlag);
		if (stat != null) {
			zk.setData(path, data, stat.getVersion());
		}
	}

	public List<String> getChildren(String path) throws Exception {
		return getChildren(path, false);
	}

	public List<String> getChildren(String path, boolean watchFlag) throws Exception {
		if (exists(path, watchFlag) != null) {
			return zk.getChildren(path, watchFlag);
		}
		return null;
	}

	public void delete(String path) throws Exception {
		delete(path, false);
	}

	public void delete(String path, boolean watchFlag) throws Exception {
		Stat stat = exists(path, watchFlag);
		if (stat != null) {
			zk.delete(path, stat.getVersion());
		}
	}
	
	public List<String> getChildrenWatcher(String path, Watcher watcher) throws Exception {
		if (exists(path, false) != null) {
			return zk.getChildren(path, watcher);
		}
		return null;
	}
}
