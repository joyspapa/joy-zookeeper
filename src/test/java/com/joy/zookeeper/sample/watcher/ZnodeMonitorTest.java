package com.joy.zookeeper.sample.watcher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.zookeeper.state.ZnodeCreateEvent;

public class ZnodeMonitorTest {
	private static final Logger logger = LoggerFactory.getLogger(ZnodeMonitorTest.class);

	String zkHosts = "192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181";
	private static Map<String, ZnodeMonitor> monitorRepo = new HashMap<>();
	private static ExecutorService executor;
	
	public static void main(String[] args) throws Exception {
		ZnodeMonitorTest test = new ZnodeMonitorTest();
		test.startMonitor();
		test.test01();
		
		
		Thread.sleep(15000);
		logger.warn("sleep(15초) : 1");
		if(executor != null) {
			executor.shutdownNow();
		}
	}
	
	public void test01() throws Exception {
		Repo.getInstance().addRepo("session01", "session01");
		
		ZnodeCreateEvent session = new ZnodeCreateEvent(zkHosts, "session01");
		logger.warn("sleep(3000) : 1");
		Thread.sleep(3000);
		logger.warn("sleep(3000) : 2");
		session.close();
	}
	
	public void startMonitor() {
		ZnodeMonitor runnableMonitor = new ZnodeMonitor(zkHosts, "/logplanet/memorygrid/timeout");
		executor = Executors.newSingleThreadExecutor();
		Future<?> future = executor.submit(runnableMonitor);
		logger.info("future.toString() : " + future.toString());
	}
	
	@Deprecated
	public void startMonitor_old2() {
		ZnodeMonitor runnableMonitor = new ZnodeMonitor(zkHosts, "/logplanet/memorygrid/timeout");
		Thread monitor = new Thread(runnableMonitor);
		monitorRepo.put("monitor_key", runnableMonitor);
		monitor.start();
	}
	
	@Deprecated
	public void startMonitor_old() {
		new Thread("ZnodeMonitor_thread") {
			public void run() {
				try {
					ZnodeMonitor executor = new ZnodeMonitor(zkHosts, "/logplanet/memorygrid/timeout");
					executor.run();
				} catch (Exception ex) {
					logger.warn(" thread warning : ", ex);
				}
			}
		}.start();
	}
}
