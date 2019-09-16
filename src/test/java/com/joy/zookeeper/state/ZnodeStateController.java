package com.joy.zookeeper.state;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.ignite.handler.CacheExpiryHandler;
import com.obzen.ignite.meta.StateMetaHandler;
import com.obzen.ignite.process.DefaultTimeoutEventProcess;
import com.obzen.kafka.core.RuntimeKafkaClientWrapperRepo;

public class ZnodeStateController extends ZnodeState {
	private static final Logger logger = LoggerFactory.getLogger(ZnodeStateController.class);
	public static ZnodeStateController instance;
	
	static {
		initialize();
		logger.info("[initialize] loading ...");
	}

	public static void initialize() {
		if (instance == null) {
			instance = new ZnodeStateController();
			instance.startController();
		}
	}
	
	public static ZnodeStateController getInstance() {
		if (instance == null) {
			initialize();
		}
		
		return instance;
	}
	
	public void doZnodeDeleteEvent(String znodeName) {
		Set<String> consumerMetaIDs = RuntimeKafkaClientWrapperRepo.getInstance().getKafkaConsumer().keySet();

		int count = 0;
		for (String consumerMetaID : consumerMetaIDs) {
			if (extractZnodeName(consumerMetaID).startsWith(znodeName)) {

				//===================================
				//TODO Area

				//startCacheExpiryHandler(consumerMetaID);
				startCacheExpiryHandlerTest();
				//===================================

				break;
			}
			count++;
		} // for end

		if (consumerMetaIDs.size() == count) {
			logger.warn("[doZnodeDeleteEvent] Not exist consumerMetaID : " + znodeName);
		}
	}

	public CacheExpiryHandler startCacheExpiryHandler(StateMetaHandler metaHandler) {
		logger.info(
				"[startCacheExpiryHandler] CacheExpiryHandler started... , consumerMetaID " + metaHandler.getMetaID());

		CacheExpiryHandler timeoutEventProcess = null;
		if (createZnodeCacheExpiry(metaHandler.getMetaID())) {
			try {
				timeoutEventProcess = new DefaultTimeoutEventProcess(metaHandler);
				timeoutEventProcess.start();
			} catch (Exception ex) {
				logger.warn("[startCacheExpiryHandler] error, consumerMetaID " + metaHandler.getMetaID(), ex);
				timeoutEventProcess.stop();
				timeoutEventProcess = null;

				removeZnodeCacheExpiry(metaHandler.getMetaID());
			}
		}

		return timeoutEventProcess;
	}

	private CacheExpiryHandler startCacheExpiryHandler(String consumerMetaID) {
		if (RuntimeKafkaClientWrapperRepo.getInstance().getKafkaConsumer(consumerMetaID).get(0).isRunning()) {
			return startCacheExpiryHandler((StateMetaHandler) RuntimeKafkaClientWrapperRepo.getInstance()
					.getKafkaConsumer(consumerMetaID).get(0).getEventHandler().getMetaHandler());
		} else {
			logger.warn("[startCacheExpiryHandler] Consumer Not Running, consumerMetaID " + consumerMetaID);
			return null;
		}
	}

	public CacheExpiryHandler startCacheExpiryHandlerTest() {
		String metaID = "DE0001-ENT001-0";
		logger.info(
				"[startCacheExpiryHandler] CacheExpiryHandler started... , consumerMetaID " + metaID);

		CacheExpiryHandler timeoutEventProcess = null;
		if (createZnodeCacheExpiry(metaID)) {
			try {
				//timeoutEventProcess = new DefaultTimeoutEventProcess(metaHandler);
				//timeoutEventProcess.start();
			} catch (Exception ex) {
				logger.warn("[startCacheExpiryHandler] error, consumerMetaID " + metaID, ex);
				timeoutEventProcess.stop();
				timeoutEventProcess = null;

				removeZnodeCacheExpiry(metaID);
			}
		}

		return timeoutEventProcess;
	}
	
	public void stopCacheExpiryHandler(StateMetaHandler metaHandler, CacheExpiryHandler timeoutEventProcess) {
		
		logger.info(
				"[stopCacheExpiryHandler] CacheExpiryHandler stopped.. , consumerMetaID " + metaHandler.getMetaID());
		stopCacheExpiryHandler(metaHandler.getMetaID(), timeoutEventProcess);
	}

	public void stopCacheExpiryHandler(String metaID, CacheExpiryHandler timeoutEventProcess) {
		logger.info(
				"[stopCacheExpiryHandler] CacheExpiryHandler stopped.. , consumerMetaID " + metaID);

		//timeoutEventProcess.stop();
		removeZnodeCacheExpiry(metaID);
	}
	
	private boolean createZnodeCacheExpiry(String metaID) {
		ZnodeCreateEvent znodeCreateEvent = null;
		boolean isCreated = false;
		String znodeName = extractZnodeName(metaID);
		try {
			znodeCreateEventMap.put(znodeName, null);
			znodeCreateEvent = new ZnodeCreateEvent(getZkHosts(), getParentZnode() + "/" + znodeName);
			if (znodeCreateEvent.createZNode() == true) {
				znodeCreateEventMap.put(znodeName, znodeCreateEvent);
				isCreated = true;
				logger.info("[createZnodeCacheExpiry] znodeName : " + znodeName);
			}
		} catch (Exception ex) {
			if (znodeCreateEvent != null) {
				znodeCreateEvent.close();
			}
			znodeCreateEventMap.remove(znodeName);
			logger.warn("[createZnodeCacheExpiry] metaID : " + metaID);
		}

		return isCreated;
	}

	private void removeZnodeCacheExpiry(String metaID) {
		String znodeName = extractZnodeName(metaID);

		znodeCreateEventMap.get(znodeName).close();
		//znodeCreateEventMap.remove(znodeName);

		logger.info("[removeZnodeCacheExpiry] znodeName : " + znodeName);
	}
}
