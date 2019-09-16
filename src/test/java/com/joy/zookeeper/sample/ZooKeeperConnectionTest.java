package com.joy.zookeeper.sample;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by inykang on 16. 6. 10.
 */
public class ZooKeeperConnectionTest {
	private static final Logger logger = LoggerFactory.getLogger(ZooKeeperConnectionTest.class);

	private ZooKeeper zk;
	final CountDownLatch connectedSignal = new CountDownLatch(1);

	public ZooKeeperConnectionTest(String zkHost) throws Exception {
		if (zkHost == null || zkHost.isEmpty()) {
			throw new IllegalArgumentException(
					"[ZooKeeperAdapterAbstract(String zkHost)] zkHost is NULL. zkHost=" + zkHost);
		}
		zk = connect(zkHost);
	}

	public void retryConnect(String zkHost) throws Exception {
		boolean isStatsOK = false;
		int maxRetryCount = 3;
		int retryCount = 1;

		while (!isStatsOK) {
			try {
				// check root node for health-check
				zk.exists("/", false);
				isStatsOK = true;

				return;
			} catch (KeeperException.ConnectionLossException ex) {
				logger.warn("Zookeeper Connection Stats is NOT OK, so It will retry to reconnect to zk. retryCount : "
						+ retryCount + " / " + maxRetryCount + ", detailMessage :" + ex.getMessage());

				if (!isStatsOK && retryCount > maxRetryCount) {
					logger.error(
							"Retrying to check the Stats of the Zookeeper Connection has exceeded the maxRetryCount. ",
							ex);
					close();
					throw ex;
				}
			} catch (Exception ex) {
				logger.warn("Zookeeper Connection critical Error : " + ex.toString());
				close();
				throw ex;
			}

			retryCount++;
		}
	}
	
	private ZooKeeper connect(String zkHost) throws IOException, InterruptedException {
		zk = new ZooKeeper(zkHost, 2000, new Watcher() {
			public void process(WatchedEvent we) {

				//logger.debug("▶ WatchedEvent process :" + we.getState());
				if (we.getState() == Event.KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});

		//connectedSignal.await(500, TimeUnit.MILLISECONDS);
		connectedSignal.await();
		return zk;
	}

	public ZooKeeper getConnect() throws IOException, InterruptedException {
		return zk;
	}
	
	/**
	 * Close the zookeeper connection
	 */
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
}
