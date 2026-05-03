import message.Message;
import message.MessageType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Gossip-based membership protocol with φ-accrual-style failure detection.
 * <p>
 * Responsibilities
 * ────────────────
 * 1. Maintain a membership table  { nodeId → MemberInfo }
 * 2. Periodically gossip our view of the table to a random subset of peers
 * 3. Detect failures: ALIVE → SUSPECTED → DEAD based on last-seen time
 * 4. Expose mergeGossip() so ConnectionHandler can apply incoming views
 */
public class MembershipManager {

    // ── tunables ──────────────────────────────────────────────────────────────
    private static final int GOSSIP_INTERVAL_MS = 1_000;   // how often we gossip
    private static final int GOSSIP_FANOUT = 3;        // peers to gossip to each round
    private static final long SUSPECT_THRESHOLD_MS = 5_000;   // no news → SUSPECTED
    private static final long DEAD_THRESHOLD_MS = 15_000;  // no news → DEAD (removed)

    // ── state ─────────────────────────────────────────────────────────────────
    private final String nodeId;
    private final String host;
    private final int port;

    /**
     * The single source of truth for membership.
     */
    private final ConcurrentHashMap<String, MemberInfo> members = new ConcurrentHashMap<>();

    /**
     * Callback so MembershipManager can send messages through the Node.
     */
    private final GossipSender sender;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Random random = new Random();

    // ── constructor ───────────────────────────────────────────────────────────
    public MembershipManager(String nodeId, String host, int port, GossipSender sender) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.sender = sender;

        // Register ourselves
        members.put(nodeId, new MemberInfo(nodeId, host, port));
    }

    public void start() {
        // Task 1: gossip our membership view periodically
        scheduler.scheduleAtFixedRate(this::gossipRound,
                GOSSIP_INTERVAL_MS, GOSSIP_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Task 2: failure detection sweep
        scheduler.scheduleAtFixedRate(this::failureDetectionSweep,
                GOSSIP_INTERVAL_MS, GOSSIP_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // ── lifecycle ─────────────────────────────────────────────────────────────

    public void stop() {
        scheduler.shutdownNow();
    }

    /**
     * Increment our own heartbeat counter then send the full membership view
     * to GOSSIP_FANOUT randomly chosen peers.
     */
    private void gossipRound() {
        try {
            // Bump our own heartbeat
            MemberInfo self = members.get(nodeId);
            if (self != null) {
                self.heartbeatCounter++;
                self.lastSeenAt = System.currentTimeMillis();
            }

            Message gossipMsg = buildGossipMessage();

            List<String> peers = sender.getPeerNodeIds();
            if (peers.isEmpty()) return;

            // Select up to GOSSIP_FANOUT peers at random
            Collections.shuffle(peers, random);
            peers.stream()
                    .limit(GOSSIP_FANOUT)
                    .forEach(pid -> sender.sendTo(pid, gossipMsg));

        } catch (Exception e) {
            System.err.println("[Membership] gossipRound error: " + e.getMessage());
        }
    }

    // ── gossip ────────────────────────────────────────────────────────────────

    private Message buildGossipMessage() {
        String view = members.values().stream()
                .filter(m -> m.status != MemberInfo.Status.DEAD)
                .map(MemberInfo::toGossipEntry)
                .collect(Collectors.joining(","));

        return new Message(
                (byte) 1,
                MessageType.GOSSIP_SYNC,
                (byte) 3,
                nodeId,
                view
        );
    }

    private void failureDetectionSweep() {
        long now = System.currentTimeMillis();
        for (MemberInfo m : members.values()) {
            if (m.nodeId.equals(nodeId)) continue; // skip self

            long gap = now - m.lastSeenAt;

            if (gap > DEAD_THRESHOLD_MS) {
                if (m.status != MemberInfo.Status.DEAD) {
                    m.status = MemberInfo.Status.DEAD;
                    System.out.printf("[Membership] ☠  Node DEAD: %s (silent %.1fs)%n",
                            m.nodeId, gap / 1000.0);
                }
                // Evict from table after marking dead
                members.remove(m.nodeId);

            } else if (gap > SUSPECT_THRESHOLD_MS) {
                if (m.status != MemberInfo.Status.SUSPECTED) {
                    m.status = MemberInfo.Status.SUSPECTED;
                    System.out.printf("[Membership] ⚠  Node SUSPECTED: %s (silent %.1fs)%n",
                            m.nodeId, gap / 1000.0);
                }
            }
        }
    }

    // ── failure detection ─────────────────────────────────────────────────────

    /**
     * Apply a GOSSIP_SYNC payload from a remote node.
     * We merge using max(heartbeatCounter) — last-write-wins per node.
     */
    public void mergeGossip(String gossipPayload) {
        if (gossipPayload == null || gossipPayload.isBlank()) return;

        for (String entry : gossipPayload.split(",")) {
            try {
                MemberInfo remote = MemberInfo.fromGossipEntry(entry);
                if (remote.nodeId.equals(nodeId)) continue; // don't overwrite self

                members.merge(remote.nodeId, remote, (existing, incoming) -> {
                    existing.updateHeartbeat(incoming.heartbeatCounter);
                    return existing;
                });
            } catch (Exception e) {
                System.err.println("[Membership] bad gossip entry: " + entry);
            }
        }
    }

    // ── merge incoming gossip ─────────────────────────────────────────────────

    /**
     * Called when a new node announces itself (GOSSIP_JOIN).
     * payload format: nodeId|host|port
     */
    public void handleJoin(String joinPayload) {
        try {
            String[] parts = joinPayload.split("\\|");
            String jId = parts[0];
            String jHost = parts[1];
            int jPort = Integer.parseInt(parts[2]);

            members.putIfAbsent(jId, new MemberInfo(jId, jHost, jPort));
            System.out.printf("[Membership] ✚  Node joined: %s @ %s:%d%n", jId, jHost, jPort);

            // Immediately gossip our full view back so the joiner learns everyone
            sender.broadcast(buildGossipMessage());
        } catch (Exception e) {
            System.err.println("[Membership] bad join payload: " + joinPayload);
        }
    }

    /**
     * Update liveness when we receive a direct heartbeat or its ACK.
     */
    public void handleHeartbeat(String fromNodeId, long counter) {
        MemberInfo m = members.get(fromNodeId);
        if (m != null) {
            m.updateHeartbeat(counter);
        }
    }

    // ── heartbeat ACK ─────────────────────────────────────────────────────────

    public Map<String, MemberInfo> getMembers() {
        return Collections.unmodifiableMap(members);
    }

    // ── queries ───────────────────────────────────────────────────────────────

    public boolean isAlive(String nid) {
        MemberInfo m = members.get(nid);
        return m != null && m.status == MemberInfo.Status.ALIVE;
    }

    /**
     * Announce ourselves to the network when first connecting.
     */
    public Message buildJoinMessage() {
        return new Message(
                (byte) 1,
                MessageType.GOSSIP_JOIN,
                (byte) 5,
                nodeId,
                nodeId + "|" + host + "|" + port
        );
    }

    public void printView() {
        System.out.println("[Membership] Current view (" + members.size() + " nodes):");
        members.values().forEach(m -> System.out.println("  " + m));
    }

    // ── interface for sending gossip ───────────────────────────────────────────
    public interface GossipSender {
        /**
         * Broadcast to all current peers.
         */
        void broadcast(Message msg);

        /**
         * Return a snapshot of current peer nodeIds for fanout selection.
         */
        List<String> getPeerNodeIds();

        /**
         * Send to a specific peer by nodeId.
         */
        void sendTo(String nodeId, Message msg);
    }
}
