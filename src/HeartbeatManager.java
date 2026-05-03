import message.Message;
import message.MessageType;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sends periodic HEARTBEAT messages to all current peers and processes
 * HEARTBEAT_ACK replies.
 * <p>
 * The heartbeat counter is separate from the gossip counter: it lets the
 * failure detector distinguish a network partition (no heartbeat) from
 * a node that is simply not generating chat traffic.
 * <p>
 * Flow:
 * every HB_INTERVAL ms  →  broadcast HEARTBEAT(counter++)
 * on receiving ACK       →  MembershipManager.handleHeartbeat(nodeId, counter)
 */
public class HeartbeatManager {

    private static final int HB_INTERVAL_MS = 2_000;

    private final String nodeId;
    private final AtomicLong counter = new AtomicLong(0);
    private final MembershipManager membership;
    private final HbSender sender;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public HeartbeatManager(String nodeId, MembershipManager membership, HbSender sender) {
        this.nodeId = nodeId;
        this.membership = membership;
        this.sender = sender;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeat,
                HB_INTERVAL_MS, HB_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    private void sendHeartbeat() {
        long seq = counter.incrementAndGet();

        // embed counter in payload so receivers can update their view of us
        Message hb = new Message(
                (byte) 1,
                MessageType.HEARTBEAT,
                (byte) 1,        // TTL=1, heartbeats are not forwarded
                nodeId,
                String.valueOf(seq)
        );

        sender.broadcast(hb);
    }

    // ── send ─────────────────────────────────────────────────────────────────

    /**
     * Called by ConnectionHandler when a HEARTBEAT arrives. Reply with ACK.
     */
    public void onHeartbeat(String fromNodeId, long remoteCounter) {
        // Update membership liveness for sender
        membership.handleHeartbeat(fromNodeId, remoteCounter);

        // Send ACK back
        Message ack = new Message(
                (byte) 1,
                MessageType.HEARTBEAT_ACK,
                (byte) 1,
                nodeId,
                String.valueOf(remoteCounter)   // echo the counter back
        );
        sender.sendTo(fromNodeId, ack);
    }

    // ── receive ───────────────────────────────────────────────────────────────

    /**
     * Called by ConnectionHandler when a HEARTBEAT_ACK arrives.
     */
    public void onHeartbeatAck(String fromNodeId, long echoedCounter) {
        membership.handleHeartbeat(fromNodeId, echoedCounter);
    }

    public interface HbSender {
        void broadcast(Message msg);

        List<String> getPeerNodeIds();

        void sendTo(String nodeId, Message msg);
    }
}