package com.obzen.zookeeper.watcher.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.ignite.handler.TimeBaseHandler;
import com.obzen.ignite.meta.StateMetaHandler;
import com.obzen.ignite.process.DefaultTimeoutEventProcess;
import com.obzen.kafka.core.meta.MetaHandler;
import com.obzen.zookeeper.watcher.ZnodeWatcher;

public class TimeoutEventProcessWatcherHandler extends ZnodeWatcherHandler {
	private static final Logger logger = LoggerFactory.getLogger(TimeoutEventProcessWatcherHandler.class);

	private MetaHandler metaHandler = null;
	private TimeBaseHandler timeoutEventProcess = null;
	private boolean isStartedTimeoutEventProcess = false; // for testing

	public TimeoutEventProcessWatcherHandler(MetaHandler metaHandler) {
		super(metaHandler.getMetaID());
		this.metaHandler = metaHandler;
	}

	public static TimeoutEventProcessWatcherHandler getInstance(MetaHandler metaHandler) {
		// StateEventHandler afterProcess 에서 호출 될때 해당 ZnodeCacheExpiryHandler를 리턴한다.
		ZnodeWatcherHandler znodeWatcherHandler = ZnodeWatcher.getInstance()
				.getExistZnodeWatcherHandler(metaHandler.getMetaID());

		if (znodeWatcherHandler == null) {
			return new TimeoutEventProcessWatcherHandler(metaHandler);
		} else {
			return (TimeoutEventProcessWatcherHandler) znodeWatcherHandler;
		}
	}

	@Override
	public void doZnodeDeleteEvent() {
		//=============================================
		//TODO User Code Area
		startTimeoutEventProcess();

		//=============================================
	}

	public void startTimeoutEventProcess() {
		if (createZnode()) {
			try {
				// zookeeper에서 직접 삭제 할 경우, 다시 자신이 이벤트 처리시에는 시작하지 않는다.
				if (!isStartedTimeoutEventProcess) {

					//=============================================
					//TODO User Code Area
					//TODO delete if 문 ,testing
					if (metaHandler instanceof StateMetaHandler) {
						timeoutEventProcess = new DefaultTimeoutEventProcess((StateMetaHandler) metaHandler);
						timeoutEventProcess.start();
					}

					//=============================================
					isStartedTimeoutEventProcess = true;
					printInfo("{} 노드에 대한 TimeoutEventProcess 시작");

				} else {
					printInfo("{} 노드에 대한 TimeoutEventProcess 시작(reuse)");
				}
			} catch (Exception ex) {
				logger.warn("{} 노드에 대한 TimeoutEventProcess 시작 실패", getZnodeName(), ex);
				stopTimeoutEventProcess(false);
			}
		} else {
			stopTimeoutEventProcess(false);
		}
	}

	private void stopTimeoutEventProcess(boolean isProcessStop) {
		if (isProcessStop) {
			removeZnode();
		}

		if (isStartedTimeoutEventProcess) {
			try {
				//=============================================
				//TODO User Code Area
				//TODO delete if 문 ,testing
				if (metaHandler instanceof StateMetaHandler) {
					timeoutEventProcess.stop();
				}

				//=============================================
				isStartedTimeoutEventProcess = false;
				printInfo("{} 노드에 대한 TimeoutEventProcess 중지");

			} catch (Exception ex) {
				logger.warn("{} 노드에 대한 TimeoutEventProcess 중지 중 에러", getZnodeName(), ex);
			} finally {
				timeoutEventProcess = null;
			}
		}
	}

	// StateEventHandler afterProcess 에서 호출 될때
	public void stopTimeoutEventProcess() {
		stopTimeoutEventProcess(true);
	}
	
	private void printInfo(String info) {
		logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		logger.info("");
		logger.info(info, getZnodeName());
		logger.info("");
		logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
	}
}
