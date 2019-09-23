package com.obzen.zookeeper.watcher;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.ignite.meta.StateMetaHandler;
import com.obzen.kafka.core.KafkaConsumerWrapper;
import com.obzen.kafka.core.RuntimeKafkaClientWrapperRepo;
import com.obzen.kafka.core.meta.MetaEntity;
import com.obzen.zookeeper.watcher.handler.TimeoutEventProcessWatcherHandler;

public class ZondeWatcherTest {
	private static final Logger logger = LoggerFactory.getLogger(ZondeWatcherTest.class);
	
	@Test
	public void startZnodeWatcher() throws Exception {
		String metaID = "DE0001-ENT001-0";
		startZnodeWatcher(metaID);
		
		TimeoutEventProcessWatcherHandler.getInstance(new StateMetaHandler(metaID, new MetaEntity())).startTimeoutEventProcess();
		
		Thread.sleep(2000);
		TimeoutEventProcessWatcherHandler.getInstance(new StateMetaHandler(metaID, new MetaEntity())).stopTimeoutEventProcess();
		
		Thread.sleep(2000);
		stopZnodeWatcher();
	}

	private void startZnodeWatcher(String metaID) {
		ZnodeWatcher.getInstance().startZnodeWatcher();
		createTempKafkaConsumer(metaID);
	}

	private void createTempKafkaConsumer(String metaID) {
		List<KafkaConsumerWrapper> consumers = new ArrayList<KafkaConsumerWrapper>();
		RuntimeKafkaClientWrapperRepo.getInstance().addKafkaConsumer(metaID, consumers);
	}
	
	private void stopZnodeWatcher() {
		System.out.println("shutdownController()");
		if (ZnodeWatcher.getInstance().getExecutor() != null) {
			ZnodeWatcher.getInstance().getExecutor().shutdownNow();
		}
	}
}
