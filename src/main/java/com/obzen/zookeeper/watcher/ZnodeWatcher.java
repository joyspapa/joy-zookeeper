package com.obzen.zookeeper.watcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.kafka.core.RuntimeKafkaClientWrapperRepo;
import com.obzen.kafka.rest.config.RestConfig;
import com.obzen.zookeeper.watcher.handler.ZnodeWatcherHandler;

public class ZnodeWatcher implements Watcher, Runnable, StatCallback, ChildrenCallback {
	private static final Logger logger = LoggerFactory.getLogger(ZnodeWatcher.class);

	private ZooKeeper zk;
	private boolean dead;
	private String zkHosts = RestConfig.getValue("zookeeper.watcher.host");
	private String parentZnode = RestConfig.getValue("zookeeper.watcher.parent.znode");
	private ExecutorService executor;
	private static ZnodeWatcher instance;
	private Map<String, ZnodeWatcherHandler> watcherHandlerMap = new HashMap<String, ZnodeWatcherHandler>();

	public static ZnodeWatcher getInstance() {
		if (instance == null) {
			instance = new ZnodeWatcher();
		}
		return instance;
	}

	public void startZnodeWatcher() {
		try {
			zk = new ZooKeeper(zkHosts, 3000, this);
			zk.getChildren(parentZnode, true, this, null);

			executor = Executors.newSingleThreadExecutor();
			executor.submit(this);

			logger.info("{} Parent 노드 ZnodeWatcher 시작됨.", parentZnode);
		} catch (IOException e) {
			logger.error("{} Parent 노드 ZnodeWatcher 시작 실패.", parentZnode, e);
		}
	}

	public void stopZnodeWatcher() {
		// close zookeeper connection
		close();

		if (executor != null) {
			try {
				executor.shutdownNow();
				logger.warn("ZnodeWatcher 중지");
			} catch (Exception ex) {
				logger.warn("ZnodeWatcher 중지 실패 [{}]", ex.toString());
			} finally {
				executor = null;
			}
		}
	}

	/*
	 * watch 걸은 노드의 상태가 변하면 callback으로 호출됨.
	 */
	@Override
	public void process(WatchedEvent event) {
		// watch건 노드의 하위노드 변경이라면 다시 watch를 건다.
		if (event.getType() == Event.EventType.NodeChildrenChanged) {
			zk.getChildren(parentZnode, true, this, null);

			logger.debug("{} 의 서브 노드 변경 감지 NodeChildrenChanged", event.getPath());

		} else if (event.getType() == Event.EventType.NodeDeleted) {
			logger.info("{} 노드 삭제 감지", event.getPath());
			doZnodeDeleteEvent(event.getPath());
		}
	}

	/*
	 * getChildren ZNODE 에 대한 callback 함수
	 */
	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children) {
		for (String znodeName : children) {
			try {
				zk.exists(parentZnode + "/" + znodeName, true, this, null);
				logger.debug("{} 노드 watcher 시작", znodeName);
			} catch (Exception e) {
				logger.error("{} 노드 watcher 시작 중 에러 : ", path, e);
			}
		}
	}

	/*
	 * ZNODE 에 대한 callback 함수
	 */
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		// logger.debug("### processResult: path=" + path + " 노드 callback...");
	}

	@Override
	public void run() {
		try {
			synchronized (this) {
				while (!isDead()) {
					wait();
				}
			}
		} catch (InterruptedException e) {
			logger.info("ZnodeWatcher end");
		}
	}

	private void close() {
		try {
			if (zk != null) {
				zk.close();
			}
		} catch (InterruptedException e) {
			logger.warn("ZooKeeper Connection 종료[{}]", e.toString());
		} finally {
			logger.warn("ZooKeeper Connection 종료");
			zk = null;
		}
	}

	public synchronized void doZnodeDeleteEvent(String znodeFullPath) {
		String znodeName = extractZnodeName(znodeFullPath);
		if (watcherHandlerMap.containsKey(znodeName)) {
			String consumerMetaID = findIsRunningKafkaConsumer(znodeName);
			if (consumerMetaID != null) {
				watcherHandlerMap.get(znodeName).doZnodeDeleteEvent();
			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug("{} 노드가 WatcherHandlerMap에 존재하지 않습니다.", znodeName);
			}
		}
	}

	public synchronized void doZnodeCreateEvent(String znodeFullPath) {
		String znodeName = extractZnodeName(znodeFullPath);
		if (watcherHandlerMap.containsKey(znodeName)) {
			String consumerMetaID = findIsRunningKafkaConsumer(znodeName);
			if (consumerMetaID != null) {
				watcherHandlerMap.get(znodeName).doZnodeCreateEvent();
			}
		}
	}

	public synchronized void doZnodeUpdateEvent(String znodeFullPath) {
		String znodeName = extractZnodeName(znodeFullPath);
		if (watcherHandlerMap.containsKey(znodeName)) {
			String consumerMetaID = findIsRunningKafkaConsumer(znodeName);
			if (consumerMetaID != null) {
				watcherHandlerMap.get(znodeName).doZnodeUpdateEvent();
			}
		}
	}

	private String findIsRunningKafkaConsumer(String znodeName) {
		Set<String> consumerMetaIDs = RuntimeKafkaClientWrapperRepo.getInstance().getKafkaConsumer().keySet();
		for (String consumerMetaID : consumerMetaIDs) {
			if (extractZnodeName(consumerMetaID).startsWith(znodeName) && RuntimeKafkaClientWrapperRepo.getInstance()
					.getKafkaConsumer(consumerMetaID).get(0).isRunning()) {
				return consumerMetaID;
			}
		} // for end

		logger.debug("{} 노드에 대한 실행중인 KafkaConsumer가 없습니다. KafkaConsumerRepo[{}]",
				znodeName, consumerMetaIDs.toString());

		return null;
	}

	public String extractZnodeName(String src) {
		String znodeName = src;

		if (znodeName.contains("/")) {
			String[] znodePaths = znodeName.split("/");
			znodeName = znodePaths[znodePaths.length - 1];
		}

		if (znodeName.contains("-")) {
			String[] znodeNames = znodeName.split("-");
			znodeName = (znodeNames.length > 1) ? znodeNames[0] + znodeNames[1] : znodeNames[0];
		}

		return znodeName;
	}

	public ZnodeWatcherHandler getExistZnodeWatcherHandler(String metaID) {
		return watcherHandlerMap.get(extractZnodeName(metaID));
	}

	public Map<String, ZnodeWatcherHandler> getWatcherHandlerMap() {
		return watcherHandlerMap;
	}

	public ZnodeWatcherHandler getWatcherHandlerMap(String znodeName) {
		return watcherHandlerMap.get(znodeName);
	}

	public void addWatcherHandlerMap(String znodeName, ZnodeWatcherHandler watcherHandler) {
		this.watcherHandlerMap.put(znodeName, watcherHandler);
	}

	public void removeWatcherHandlerMap(String znodeName) {
		this.watcherHandlerMap.remove(znodeName);
	}

	public boolean isDead() {
		return dead;
	}

	public void setDead(boolean dead) {
		this.dead = dead;
	}

	public String getZkHosts() {
		return zkHosts;
	}

	public void setZkHosts(String zkHosts) {
		this.zkHosts = zkHosts;
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public String getParentZnode() {
		return parentZnode;
	}
}
