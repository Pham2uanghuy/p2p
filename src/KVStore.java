import message.Message;
import message.MessageType;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Eventually-consistent distributed key-value store.
 * <p>
 * Consistency model
 * ─────────────────
 * • Writes: last-write-wins (LWW) resolved by (timestamp, writerNodeId).
 * • Reads:  local (no quorum) – stale reads are possible during partition.
 * • Anti-entropy: every ANTI_ENTROPY_INTERVAL ms we gossip our full store
 * to a random peer, so diverged replicas converge over time.
 * <p>
 * Message flow
 * ────────────
 * PUT  →  STORE_PUT broadcast  →  every node merges via applyRemotePut()
 * GET  →  answered locally     →  no network round-trip for reads
 */
public class KVStore {

    private static final int ANTI_ENTROPY_INTERVAL_MS = 5_000;

    private final String nodeId;
    private final ConcurrentHashMap<String, VersionedValue> store = new ConcurrentHashMap<>();
    private final StoreSender sender;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public KVStore(String nodeId, StoreSender sender) {
        this.nodeId = nodeId;
        this.sender = sender;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::antiEntropyRound,
                ANTI_ENTROPY_INTERVAL_MS, ANTI_ENTROPY_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    /**
     * Write a key locally and broadcast the put to all peers.
     */
    public void put(String key, String value) {
        VersionedValue vv = new VersionedValue(value, System.currentTimeMillis(), nodeId);
        applyLocally(key, vv);

        // propagate
        Message msg = new Message(
                (byte) 1,
                MessageType.STORE_PUT,
                (byte) 5,
                nodeId,
                "",          // payload unused
                key,
                vv.serialize()
        );
        sender.broadcast(msg);
        System.out.printf("[KVStore] PUT %s=%s (ts=%d)%n", key, value, vv.timestamp);
    }

    // ── local write ───────────────────────────────────────────────────────────

    /**
     * Returns null if key not found.
     */
    public Optional<VersionedValue> get(String key) {
        return Optional.ofNullable(store.get(key));
    }

    // ── local read ────────────────────────────────────────────────────────────

    public void printStore() {
        System.out.println("[KVStore] Local store (" + store.size() + " entries):");
        store.forEach((k, v) -> System.out.printf("  %s = %s%n", k, v));
    }

    /**
     * Called by ConnectionHandler on STORE_PUT.
     * Merge using LWW; do NOT re-broadcast (the original sender already did).
     */
    public void applyRemotePut(String key, String serializedValue) {
        VersionedValue incoming = VersionedValue.deserialize(serializedValue);
        boolean updated = applyLocally(key, incoming);
        if (updated) {
            System.out.printf("[KVStore] ← replicated PUT %s=%s (ts=%d, from=%s)%n",
                    key, incoming.value, incoming.timestamp, incoming.writerNodeId);
        }
    }

    // ── remote put ────────────────────────────────────────────────────────────

    /**
     * Push our entire store snapshot to one random peer.
     * The peer merges with its own copy — this is how diverged replicas converge.
     */
    private void antiEntropyRound() {
        if (store.isEmpty()) return;

        List<String> peers = sender.getPeerNodeIds();
        if (peers.isEmpty()) return;

        String target = peers.get(new Random().nextInt(peers.size()));

        // Pack every entry into one message: payload = "k1§v1,k2§v2,..."
        StringBuilder sb = new StringBuilder();
        store.forEach((k, vv) -> {
            if (sb.length() > 0) sb.append(",");
            sb.append(k).append("§").append(vv.serialize());
        });

        Message msg = new Message(
                (byte) 1,
                MessageType.GOSSIP_SYNC,  // reuse type; payload distinguishes it
                (byte) 1,
                nodeId,
                "__STORE_SYNC__:" + sb
        );
        sender.sendTo(target, msg);
    }

    // ── anti-entropy (full-state gossip) ──────────────────────────────────────

    /**
     * Merge a full-state store sync from a remote node.
     * Called by ConnectionHandler when payload starts with "__STORE_SYNC__:".
     */
    public void mergeRemoteStore(String payload) {
        String data = payload.substring("__STORE_SYNC__:".length());
        if (data.isBlank()) return;

        for (String entry : data.split(",")) {
            int sep = entry.indexOf("§");
            if (sep < 0) continue;
            String k = entry.substring(0, sep);
            String vs = entry.substring(sep + 1);
            try {
                applyLocally(k, VersionedValue.deserialize(vs));
            } catch (Exception e) {
                System.err.println("[KVStore] bad sync entry: " + entry);
            }
        }
    }

    /**
     * Returns true if the incoming value actually updated our local copy.
     */
    private boolean applyLocally(String key, VersionedValue incoming) {
        VersionedValue[] updated = {null};
        store.merge(key, incoming, (existing, inc) -> {
            if (inc.winsOver(existing)) {
                updated[0] = inc;
                return inc;
            }
            return existing;
        });
        return updated[0] != null;
    }

    // ── internal ─────────────────────────────────────────────────────────────

    public interface StoreSender {
        void broadcast(Message msg);

        List<String> getPeerNodeIds();

        void sendTo(String nodeId, Message msg);
    }
}