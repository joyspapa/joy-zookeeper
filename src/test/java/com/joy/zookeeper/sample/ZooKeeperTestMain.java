package com.joy.zookeeper.sample;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperTestMain {

	private static final Logger logger = LoggerFactory.getLogger(ZooKeeperTestMain.class);
	private static String zkHosts = "192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181";
	private String zkNodeRoot = "/logplanet/memorygrid/timeout";
	private boolean isRunningWatcher;

	public static void main(String[] args) throws Exception {
		logger.debug(" start : ");
		ZooKeeperTestMain main = new ZooKeeperTestMain();
		//main.startWatcher();

		//main.test01();
		main.test02();
		//main.testDelete();
		Thread.sleep(10000);
		ZooKeeperHandlerTest.getInstance(zkHosts).closeConnection();
		logger.debug(" end : ");
	}

	public void startWatcher() throws Exception {
		new Thread("startWwatcher") {
			public void run() {
				isRunningWatcher = true;
				try {

					ZooKeeperHandlerTest.getInstance(zkHosts).getChildrenWatcher(zkNodeRoot, new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							if (event.getType() == Event.EventType.NodeChildrenChanged) {
								String path = event.getPath();
								String eventType = event.getType().name();
								String eventState = event.getState().name();
								logger.info("서버 복구 통지 및 처리 : path = " + path + ", eventType=" + eventType
										+ ", eventState=" + eventState);

								try {
									ZooKeeperHandlerTest.getInstance(zkHosts).getChildrenWatcher(zkNodeRoot, this);
									//									if (ZooKeeperHandlerTest.getInstance(zkHosts).exists(serverPath, true) != null) {
									//
									//										// TODO 서버 복구 통지 및 처리
									//										// adminService.alive(serverPath);
									//										logger.info("서버 복구 통지 및 처리 : " + serverPath);
									//									} else {
									//										// TODO 서버 장애 통지 및 처리
									//										// adminService.dead(serverPath);
									//										logger.info("서버 장애 통지 및 처리 : ");
									//									}
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
									logger.warn(" 통지 및 처리 thread warning : ", e);
								}
							} else {
								logger.info("서버 복구 통지 및 처리 : else ");
							}
						}
					});

					while (isRunningWatcher) {
						Thread.sleep(3000);
						logger.info("서버 통지 감시중...");
					}

				} catch (Exception ex) {
					logger.warn(" startWatcher thread warning : ", ex);
				}
			}
		}.start();
	}

	public void testDelete() throws Exception {

		try {
			ZooKeeperHandlerTest.getInstance(zkHosts).delete(zkNodeRoot + "/case99");

			logger.debug("testDelete end : ");
		} catch (Exception ex) {
			logger.warn(" thread warning : ", ex);
		}
	}
	
	public void test03() throws Exception {

		try {
			ZooKeeperHandlerTest.getInstance(zkHosts).create(zkNodeRoot + "/case99", "test99".getBytes(),
					CreateMode.PERSISTENT);

			logger.debug(" thread end : ");
		} catch (Exception ex) {
			logger.warn(" thread warning : ", ex);
		}
	}

	public void test02() throws Exception {
		new Thread("test02") {
			public void run() {
				try {
					ZooKeeperHandlerTest.getInstance(zkHosts).create(zkNodeRoot + "/case1", "test".getBytes(),
							CreateMode.EPHEMERAL);

					Thread.sleep(3000);
					ZooKeeperHandlerTest.getInstance(zkHosts).closeConnection();
					logger.debug(" thread end : ");
				} catch (Exception ex) {
					logger.warn(" thread warning : ", ex);
				}
			}
		}.start();
	}

	public void test01() throws Exception {
		for (int i = 0; i < 200; i++) {
			//try {
			Stat stat = ZooKeeperHandlerTest.getInstance(zkHosts).exists("/logplanet/memorygrid/timeout");

			if (stat != null) {
				logger.debug(i + " stat.getVersion() : " + stat.getVersion());
				Thread.sleep(1000);
			} else {
				logger.debug(i + " stat is NULL !");
			}
			//			} catch (ConnectionLossException lossEx) {
			//				logger.warn(i + " ConnectionLossException !", lossEx);
			//			} catch (Exception ex) {
			//				logger.warn(i + " Exception !", ex);
			//			}
		}
	}
}
