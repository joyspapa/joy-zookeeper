package com.obzen.zookeeper.watcher.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.zookeeper.event.ZnodeCreateEvent;
import com.obzen.zookeeper.watcher.ZnodeWatcher;

public abstract class ZnodeWatcherHandler {
	private static final Logger logger = LoggerFactory.getLogger(ZnodeWatcherHandler.class);

	private ZnodeCreateEvent znodeCreateEvent = null;
	private boolean isRunning = false;
	private String znodeName = null;

	public ZnodeWatcherHandler(String metaID) {
		this.znodeName = ZnodeWatcher.getInstance().extractZnodeName(metaID);
	}

	// TOOD Override
	public abstract void doZnodeDeleteEvent();

	// TOOD
	public void doZnodeCreateEvent() {
	}

	// TOOD
	public void doZnodeUpdateEvent() {
	}

	protected boolean createZnode() {
		boolean isCreated = false;
		try {
			// zookeeper에서 직접 삭제 할 경우, 다시 자기 자신이 이벤트 처리시 기존 ZnodeCreateEvent 제거
			if (isRunning) {
				logger.debug("{} 노드생성 전 기존 ZnodeCreateEvent 제거", znodeName);
				removeZnode();
			}

			// CacheExpiry znode 생성
			znodeCreateEvent = new ZnodeCreateEvent(ZnodeWatcher.getInstance().getZkHosts(),
					ZnodeWatcher.getInstance().getParentZnode() + "/" + znodeName);
			if (znodeCreateEvent.createZNode() == true) {
				isCreated = true;
				isRunning = true;
			}

			ZnodeWatcher.getInstance().addWatcherHandlerMap(znodeName, this);

		} catch (Exception ex) {
			removeZnode();
			logger.warn("{} 노드생성 실패 , warning : ", znodeName, ex);
		}

		return isCreated;
	}

	protected void removeZnode() {
		ZnodeWatcher.getInstance().removeWatcherHandlerMap(znodeName);
		isRunning = false;
		znodeCreateEvent.close();

		logger.debug("{} 노드 삭제", znodeName);
	}

	public String getZnodeName() {
		return znodeName;
	}

	public boolean isRunning() {
		return isRunning;
	}
}
