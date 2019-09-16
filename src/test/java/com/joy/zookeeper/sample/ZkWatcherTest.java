package com.joy.zookeeper.sample;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkWatcherTest implements Watcher, Runnable, StatCallback, ChildrenCallback {
	// 주키퍼 세션을 유지하는 주키퍼 객체
	private ZooKeeper zk;

	private boolean dead;

	/**
	 * IP of zookeeper server must be static. you can use zookeeper ensemble for
	 * high reliability and availability.
	 */
	public static final String ZOOKEEPER_SERVER_IP = "192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181";

	// 주키퍼 세션 시작하기.
	public void initializeSession() {
		try {
			// 연결한다
			zk = new ZooKeeper(ZOOKEEPER_SERVER_IP, 3000, this);

			// 이 자바소스코드로 연결되어있을동안 활성화될 ephemeral 노드를 생성한다. 속성은 Sequential
			// 첫 번째 인자가 생성될 위치, 두 번째 인자가 노드의 데이터
			//zk.create("/logplanet/memorygrid/timeout/case1", "테스트 데이터입니다".getBytes(),
			//        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

			// zk.getChildren으로 /client의 하위 노드를 얻어오고 watch를 건다.
			zk.getChildren("/logplanet/memorygrid/timeout", true, this, null);
		}

		catch (IOException e) {
			e.printStackTrace();
		}
		//        catch (KeeperException e) {
		//            e.printStackTrace();
		//        } 
		//        catch (InterruptedException e) {
		//            e.printStackTrace();
		//        }
	}

	// watch 걸은 노드의 상태가 변하면 callback으로 호출됨.
	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();

		String eventType = event.getType().name();

		String eventState = event.getState().name();

		System.out.println(
				"## process: path : " + path + ", " + "eventType : " + eventType + ", eventState: " + eventState);

		// watch건 노드의 하위노드 변경이라면 다시 watch를 건다.
		if (event.getType() == Event.EventType.NodeChildrenChanged) {
			zk.getChildren("/logplanet/memorygrid/timeout", true, this, null);
		} else if (event.getType() == Event.EventType.NodeDeleted) {
			System.out.println(
					"## process: path : " + path + " 노드가 삭제 되었습니다.");
		}

	}

	// getChildren함수의 callback 함수
	@Override
	public void processResult(int arg0, String arg1, Object arg2, List<String> arg3) {

		for (String child : arg3) {

			//byte[] data = null;
			try {
				// 실제로 데이터를 가져오는 부분
				//data = zk.getData("/logplanet/memorygrid/timeout" + "/" + child, true, null);

				zk.exists("/logplanet/memorygrid/timeout" + "/" + child, true, this, null);
			}catch (Exception e) {
				// We don't need to worry about recovering now. The watch
				// callbacks will kick off any exception handling
				e.printStackTrace();
			}
			
//			catch (KeeperException e) {
//				// We don't need to worry about recovering now. The watch
//				// callbacks will kick off any exception handling
//				e.printStackTrace();
//			}
//			catch (InterruptedException e) {
//				return;
//			}
			System.out.println("### processResult: rc=" + arg0 + ", " + "path=" + arg1 + ", child= " + child);
			//			System.out.println("## processResult: rc=" + arg0 + ", " + "path=" + arg1 + ", child= " + child
			//					+ ", data = " + new String(data));
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		System.out.println("#### processResult: rc=" + rc + ", " + "path=" + path + ", stat= " + stat.getCzxid());
	}

	// 대기...
	@Override
	public void run() {
		try {
			synchronized (this) {
				while (!dead) {
					wait();
				}
			}
		}

		catch (InterruptedException e) {
		}
	}

	public static void main(String args[]) {
		ZkWatcherTest test = new ZkWatcherTest();
		test.initializeSession();

		test.run();
	}

}