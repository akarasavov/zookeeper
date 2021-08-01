package io.atk;

import com.sun.tools.javac.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LeaderElectionTest extends ClientBase {

    private static final String TEST_ROOT_NODE = "/" + System.currentTimeMillis();
    private ZooKeeper zooKeeper;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        zooKeeper = createClient();
        zooKeeper.create(
                TEST_ROOT_NODE,
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zooKeeper.close();

    }

    @Test
    public void tryToElectLeader() throws InterruptedException, IOException {
        final List<Node> nodes = List.of(new Node(createClient(), TEST_ROOT_NODE, "1"),
                new Node(createClient(), TEST_ROOT_NODE, "2"),
                new Node(createClient(), TEST_ROOT_NODE, "3"));

        for (Node node : nodes) {
            new Thread(node).start();
        }
        for (int i = 0; i < 2; i++) {
            Thread.sleep(3000);
            nodes.stream().filter(Node::isLeader).findFirst().ifPresent(Node::stop);
        }
        System.out.println("Start sleeping!!!");
        Thread.sleep(100000);
    }
}