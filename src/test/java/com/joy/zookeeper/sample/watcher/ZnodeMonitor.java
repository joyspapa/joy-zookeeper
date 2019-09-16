package com.joy.zookeeper.sample.watcher;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.zookeeper.state.ZnodeCreateEvent;

public class ZnodeMonitor implements Watcher, Runnable, StatCallback, ChildrenCallback {
	private static final Logger logger = LoggerFactory.getLogger(ZnodeMonitor.class);

	private ZooKeeper zk;
	private boolean dead;
	private String zkHosts;
	private String parentZnode;

	public ZnodeMonitor(String zkHosts, String watchRootZnode) {
		this.zkHosts = zkHosts;
		this.parentZnode = watchRootZnode;

		initializeSession();
	}

	// 주키퍼 세션 시작하기.
	public void initializeSession() {
		try {
			zk = new ZooKeeper(zkHosts, 3000, this);
			zk.getChildren(parentZnode, true, this, null);
			logger.info(parentZnode + " 노드 watcher 시작...");
		} catch (IOException e) {
			logger.error("initializeSession error : ", e);
		}
	}

	/*
	 * getChildren 함수의 callback 함수
	 */
	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children) {
//		if (logger.isDebugEnabled()) {
//			logger.debug("## processResult: path=" + path + " 서브노드 callback...");
//		}
		for (String child : children) {
			try {
				zk.exists(parentZnode + "/" + child, true, this, null);
				//logger.info("## processResult: child=" + child + " watcer 실행");
			} catch (Exception e) {
				logger.error("## processResult error : ", e);
			}
		}
		//logger.debug("## processResult: ===============================");
	}

	/*
	 * exist 함수의 callback 함수
	 */
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
//		if (logger.isDebugEnabled()) {
//			logger.debug("### processResult: path=" + path + " 노드 callback...");
//		}
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
			logger.error("run() error : ", e);
			close();
		}
	}

	/*
	 * watch 걸은 노드의 상태가 변하면 callback으로 호출됨.
	 */
	@Override
	public void process(WatchedEvent event) {

		// watch건 노드의 하위노드 변경이라면 다시 watch를 건다.
		if (event.getType() == Event.EventType.NodeChildrenChanged) {
			if (logger.isDebugEnabled()) {
				logger.debug("# process: path : " + event.getPath() + " 서브노드 변경됨.");
			}
			zk.getChildren(parentZnode, true, this, null);
		} else if (event.getType() == Event.EventType.NodeDeleted) {
			action(event.getPath());
		}
//		else {
//			logger.warn("# process: path : " + event.getPath() + " 노드. Unknown Event.EventType=" + event.getType());
//		}
	}

	public void action(String path) {
		logger.info("# process: path : " + path + " 노드가 삭제 되었습니다.");
		String[] splited = path.split("/");
		if(splited.length > 0) {
			String znode = splited[splited.length-1];
			logger.info("# process: 삭제된 노드 : " + znode);
			String value = Repo.getInstance().getRepo(znode);
			if(value != null) {
				new ZnodeCreateEvent(zkHosts, value);
			}
		}
	}
	
	public boolean isDead() {
		return dead;
	}

	public void setDead(boolean dead) {
		this.dead = dead;
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

	public static void main(String args[]) throws Exception {
//		String ip = "192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181";
//		ZnodeMonitor executor = new ZnodeMonitor(ip, "/logplanet/memorygrid/timeout");
//		executor.run();

		logger.info("end...");
	}
}
