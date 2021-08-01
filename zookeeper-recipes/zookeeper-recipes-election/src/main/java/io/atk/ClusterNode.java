package io.atk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class ClusterNode implements Watcher, Runnable {
    private Logger logger = LoggerFactory.getLogger(ClusterNode.class);
    private final ZooKeeper zookeeper;
    private final String hostName;
    private final String lockPath;
    private BlockingQueue<Message> channel = new LinkedBlockingQueue<>();
    private volatile boolean leader;

    public ClusterNode(ZooKeeper zooKeeper, String parentPath, String hostName) {
        this.zookeeper = zooKeeper;
        this.hostName = hostName;
        this.lockPath = parentPath.toString() + "/" + "lock";
        channel.add(new TryToBecomeLeader());
    }

    @Override
    public void run() {
        try {
            zookeeper.addWatch(lockPath, this, AddWatchMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error on creating watch", e);
            throw new RuntimeException(e);
        }
        while (true) {
            final Message message = nextMessage();
            logger.info("Next msg {}", message);
            if (message instanceof StopNode) {
                closeClient();
                break;
            }
            if (message instanceof TryToBecomeLeader) {
                final boolean createNode = createNode(lockPath, hostName);
                if (createNode) {
                    logger.info("Node with hostname {} is a leader", hostName);
                    leader = true;
                } else {
                    try {
                        final String leaderHostName =
                                new String(zookeeper.getData(lockPath, false, new Stat()), UTF_8);
                        logger.info("Node with hostname {} failed to become a leader. Leader is {}", hostName, leaderHostName);
                    } catch (Exception ex) {
                        logger.error("Failed to getData on path {}", lockPath, ex);
                    }
                }
            }
        }
    }

    private void closeClient() {
        try {
            logger.info("Node with hostname {} will be stopped", hostName);
            zookeeper.close();
            logger.info("Node with hostname {} stopped", hostName);
        } catch (InterruptedException e) {
            logger.error("Error to close client", e);
        }
    }

    public void stop() {
        logger.info("Node with hostname {} will be stopped", hostName);
        channel.add(new StopNode());
    }

    public boolean isLeader() {
        return leader;
    }

    private Message nextMessage() {
        try {
            return channel.take();
        } catch (InterruptedException e) {
            return new StopNode();
        }
    }

    private boolean createNode(String lockPath, String hostName) {
        try {
            zookeeper.create(lockPath, hostName.getBytes(UTF_8),
                    OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException | InterruptedException e) {
            logger.error("Wasn't able to create node", e);
            return false;
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Received event {}", event);
        if (event.getPath() != null && event.getPath().equals(lockPath) && event.getType() == Event.EventType.NodeDeleted) {
            channel.add(new TryToBecomeLeader());
        }
    }

    private static interface Message {
    }

    private static class StopNode implements Message {
    }

    private static class TryToBecomeLeader implements Message {
    }
}
