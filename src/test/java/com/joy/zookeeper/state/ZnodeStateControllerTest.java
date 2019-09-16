package com.joy.zookeeper.state;

import java.util.ArrayList;
import java.util.List;

import com.obzen.ignite.handler.CacheExpiryHandler;
import com.obzen.kafka.core.KafkaConsumerWrapper;
import com.obzen.kafka.core.RuntimeKafkaClientWrapperRepo;

public class ZnodeStateControllerTest {

	public static void main(String[] args) throws Exception {
		ZnodeStateControllerTest test = new ZnodeStateControllerTest();
		test.startZnodeStateController();
	}

	public void startZnodeStateController() throws Exception {
		// ZnodeStateController initialize
		try {
			ZnodeStateController.getInstance();

			String metaID = "DE0001-ENT001-0";
			List<KafkaConsumerWrapper> consumers = new ArrayList<KafkaConsumerWrapper>();
			RuntimeKafkaClientWrapperRepo.getInstance().addKafkaConsumer(metaID, consumers);

			//Thread.sleep(30000);

			// ZnodeStateController 시작
			CacheExpiryHandler cacheExpiryHandler = ZnodeStateController.getInstance().startCacheExpiryHandlerTest();
			Thread.sleep(5000);
			ZnodeStateController.getInstance().stopCacheExpiryHandler(metaID, cacheExpiryHandler);
			//RuntimeKafkaClientWrapperRepo.getInstance().removeKafkaConsumer("case66");

			Thread.sleep(40000);
			
		} finally {
			shutdownController();
		}
	}

	public void shutdownController() {
		System.out.println("shutdownController()");
		if (ZnodeStateController.getInstance().getExecutor() != null) {
			ZnodeStateController.getInstance().getExecutor().shutdownNow();
		}
	}
}
