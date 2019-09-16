package com.joy.zookeeper.state;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.kafka.core.RuntimeKafkaClientWrapperRepo;

public abstract class ZnodeState implements Watcher, Runnable, StatCallback, ChildrenCallback {
	private static final Logger logger = LoggerFactory.getLogger(ZnodeState.class);

	private ZooKeeper zk;
	private boolean dead;
	private String zkHosts = "192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181";
	private String parentZnode = "/logplanet/memorygrid/timeoutevent";
	private ExecutorService executor;
	
	public Map<String, ZnodeCreateEvent> znodeCreateEventMap = new HashMap<String, ZnodeCreateEvent>();

	public void startController() {
		try {
			zk = new ZooKeeper(zkHosts, 3000, this);
			zk.getChildren(parentZnode, true, this, null);

			executor = Executors.newSingleThreadExecutor();
			Future<?> future = executor.submit(this);

			logger.info("[startController] " + parentZnode + " ZnodeState watcher starting future result : "
					+ future.toString());

		} catch (IOException e) {
			logger.error("[startController] " + parentZnode + " startController error : ", e);
		}
	}

	/*
	 * getChildren ZNODE 에 대한 callback 함수
	 */
	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children) {
		logger.info("## processResult: path=" + path);
		for (String znodeName : children) {
			try {
				if (hasKafkaConsumer(znodeName) && znodeCreateEventMap.containsKey(znodeName)) {
					zk.exists(parentZnode + "/" + znodeName, true, this, null);
					logger.info("## processResult: znodeName=" + znodeName + " watcher running ...");
				} else {
					logger.info("## processResult: znodeName=" + znodeName + " watcher skipped ...");
				}
			} catch (Exception e) {
				logger.error("## processResult error : ", e);
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

			if (logger.isDebugEnabled()) {
				logger.debug("# process: path : " + event.getPath() + " 서브노드 변경됨.");
			}
		} else if (event.getType() == Event.EventType.NodeDeleted) {
			logger.info("# process: path : " + event.getPath() + " 노드 삭제 감지");

			doZnodeDeleteEvent(extractZnodeName(event.getPath()));
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
			logger.warn("[run] stopped ! InterruptedException : " + e.toString());
			close();
		}
	}
	
	public void close() {
		try {
			if (zk != null) {
				zk.close();
			}
		} catch (InterruptedException e) {
			logger.warn("[close] zoo.close() InterruptedException : " + e.toString());
		} finally {
			zk = null;
		}
	}

	abstract void doZnodeDeleteEvent(String znodeName);

	public boolean hasKafkaConsumer(String znodeName) {
		boolean isHas = false;
		
		Set<String> consumerMetaIDs = RuntimeKafkaClientWrapperRepo.getInstance().getKafkaConsumer().keySet();
		for (String consumerMetaID : consumerMetaIDs) {
			if (extractZnodeName(consumerMetaID).startsWith(znodeName)) {
				isHas = true;
				break;
			}
		} // for end

		if (!isHas) {
			logger.warn("[hasKafkaConsumer] Not exist consumerMetaID : " + znodeName);
		}
		
		return isHas;
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
		
		//logger.info("[extractZnodeName] znodeName : " + znodeName);
		return znodeName;
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
