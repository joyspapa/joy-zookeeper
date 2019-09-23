package com.joy.zookeeper.event;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
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
		
		connect();
	}
	
	private void connect() {
		try {
			zk = new ZooKeeper(zkHosts, 2000, new Watcher() {
				public void process(WatchedEvent we) {
					if (we.getState() == Event.KeeperState.SyncConnected) {
						connectedSignal.countDown();
					}
				}
			});
			
			connectedSignal.await(500, TimeUnit.MILLISECONDS);
			
		} catch (Throwable e) {
			logger.warn("{} 노드 생성을 위한 Zookeeper Connection 실패", znodeName, e);
			close();
		}
	}
	
	public boolean createZNode() {
		boolean isCreatedZnode = false;
		try {
			if (zk.exists(znodeName, false) == null) {
				zk.create(znodeName, InetAddress.getLocalHost().getHostName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
				isCreatedZnode = true;
				
				logger.info("{} 노드 생성", znodeName);
				
			} else {
				logger.debug("{} 노드 이미 존재[NodeExists]", znodeName);
				close();
			}
		} catch (Throwable e) {
			if(e instanceof NodeExistsException) {
				logger.debug("{} 노드 이미 존재[NodeExists]", znodeName);
			} else {
				logger.warn("{} 노드 생성 실패. error : ", znodeName, e);
			}
			close();
		}
		
		return isCreatedZnode;
	}
	
	public void close() {
		try {
			if (zk != null) {
				zk.close();
			}
		} catch (InterruptedException e) {
			logger.info("ZooKeeper Connection 종료");
		} finally {
			zk = null;
		}
	}

	public String getZnodeName() {
		return znodeName;
	}
}
