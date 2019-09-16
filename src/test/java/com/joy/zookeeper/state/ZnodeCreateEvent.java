package com.joy.zookeeper.state;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZnodeCreateEvent {
	private static final Logger logger = LoggerFactory.getLogger(ZnodeCreateEvent.class);

	private ZooKeeper zk;
	private final CountDownLatch connectedSignal = new CountDownLatch(1);

	private String zkHosts;
	private String znodeName;
	private CreateMode createMode;
	
	public ZnodeCreateEvent(String zkHosts, String znodeName) {
		this(zkHosts, znodeName, CreateMode.EPHEMERAL);
	}

	public ZnodeCreateEvent(String zkHosts, String znodeName, CreateMode createMode) {
		this.zkHosts = zkHosts;
		this.znodeName = znodeName;
		this.createMode = createMode;
	}
	
	public boolean createZNode() {
		boolean isCreatedZnode = false;
		try {
			zk = new ZooKeeper(zkHosts, 2000, new Watcher() {
				public void process(WatchedEvent we) {
					if (we.getState() == Event.KeeperState.SyncConnected) {
						connectedSignal.countDown();
					}
				}
			});
			connectedSignal.await(500, TimeUnit.MILLISECONDS);
			
			if (zk.exists(znodeName, false) == null) {
				zk.create(znodeName, InetAddress.getLocalHost().getHostName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
				isCreatedZnode = true;
			} else {
				logger.warn("Aleady existing Znode : " + znodeName + " , To create this znode will be skipped!");
				close();
			}
		} catch (Throwable e) {
			logger.error("createZNode error Znode : " + znodeName, e);
			close();
		}
		
		if(logger.isInfoEnabled()) {
			logger.info("createZNode isCreatedZnode : " + isCreatedZnode + " , Znode = " + znodeName);
		}
		
		return isCreatedZnode;
	}
	
	public void close() {
		try {
			if (zk != null) {
				zk.close();
			}
		} catch (InterruptedException e) {
			logger.warn(">>> [close] zoo.close() InterruptedException : " + e.toString());
		} finally {
			zk = null;
		}
	}

	public String getZnodeName() {
		return znodeName;
	}
}
