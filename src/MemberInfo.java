/**
 * Tracks liveness state for one peer node.
 * <p>
 * Gossip carries a heartbeat counter (monotonically increasing) and the
 * wall-clock time we last received a bump for this node.  Failure detection
 * uses the wall-clock gap; the counter lets us distinguish stale gossip
 * (counter hasn't moved) from a genuinely live node.
 */
public class MemberInfo {

    public final String nodeId;
    public final String host;
    public final int port;
    // — heartbeat state —
    public volatile long heartbeatCounter;   // incremented by the owner node
    public volatile long lastSeenAt;         // System.currentTimeMillis() when we last heard from it
    public volatile Status status;
    public MemberInfo(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.heartbeatCounter = 0;
        this.lastSeenAt = System.currentTimeMillis();
        this.status = Status.ALIVE;
    }

    public static MemberInfo fromGossipEntry(String entry) {
        String[] parts = entry.split("\\|");
        MemberInfo m = new MemberInfo(parts[0], parts[1], Integer.parseInt(parts[2]));
        m.heartbeatCounter = Long.parseLong(parts[3]);
        return m;
    }

    /**
     * Called whenever we receive a heartbeat (direct or via gossip) with a fresher counter.
     */
    public synchronized void updateHeartbeat(long counter) {
        if (counter > this.heartbeatCounter) {
            this.heartbeatCounter = counter;
            this.lastSeenAt = System.currentTimeMillis();
            this.status = Status.ALIVE;
        }
    }

    /**
     * Serialise to a compact string suitable for embedding in a gossip payload.
     * Format:  nodeId|host|port|heartbeatCounter
     */
    public String toGossipEntry() {
        return nodeId + "|" + host + "|" + port + "|" + heartbeatCounter;
    }

    @Override
    public String toString() {
        return String.format("Member{id=%s, addr=%s:%d, hb=%d, status=%s}",
                nodeId, host, port, heartbeatCounter, status);
    }

    public enum Status {ALIVE, SUSPECTED, DEAD}
}