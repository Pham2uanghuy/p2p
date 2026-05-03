import message.Message;
import message.MessageType;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads messages from one peer and dispatches them to the correct subsystem.
 * <p>
 * Message routing
 * ───────────────
 * CHAT            → print + forward (original behaviour)
 * GOSSIP_JOIN     → MembershipManager.handleJoin()
 * GOSSIP_SYNC     → MembershipManager.mergeGossip() or KVStore.mergeRemoteStore()
 * HEARTBEAT       → HeartbeatManager.onHeartbeat()
 * HEARTBEAT_ACK   → HeartbeatManager.onHeartbeatAck()
 * STORE_PUT       → KVStore.applyRemotePut()
 */
public class ConnectionHandler implements Runnable {

    // shared dedup cache across all handlers
    private static final Set<String> seenMessages =
            Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Peer peer;
    private final Node node;

    public ConnectionHandler(Peer peer, Node node) {
        this.peer = peer;
        this.node = node;
    }

    @Override
    public void run() {
        try (DataInputStream in = new DataInputStream(peer.getSocket().getInputStream())) {

            while (true) {
                int length = in.readInt();
                byte[] data = new byte[length];
                in.readFully(data);

                Message msg = Message.fromBytes(data);

                // ── version check ─────────────────────────────────────────
                if (msg.version != 1) {
                    System.err.println("[Handler] Unsupported version: " + msg.version);
                    continue;
                }

                // ── TTL check ─────────────────────────────────────────────
                if (msg.ttl <= 0) continue;

                // ── dedup (skip heartbeats — they are not forwarded) ──────
                boolean isHb = msg.type == MessageType.HEARTBEAT ||
                        msg.type == MessageType.HEARTBEAT_ACK;
                if (!isHb && seenMessages.contains(msg.id)) continue;
                if (!isHb) seenMessages.add(msg.id);

                // ── dispatch ──────────────────────────────────────────────
                dispatch(msg);
            }

        } catch (IOException e) {
            System.out.printf("[Handler] Peer %s disconnected%n",
                    peer.getRemoteNodeId() != null ? peer.getRemoteNodeId() : peer.getSocket().getRemoteSocketAddress());
            node.removePeer(peer);
        }
    }

    private void dispatch(Message msg) {
        MembershipManager membership = node.getMembershipManager();
        HeartbeatManager heartbeat = node.getHeartbeatManager();
        KVStore store = node.getKVStore();

        switch (msg.type) {

            // ── chat (original behaviour) ─────────────────────────────────
            case MessageType.CHAT -> {
                System.out.printf("[Chat] %s: %s%n", msg.originalId, msg.payload);
                msg.ttl--;
                node.broadcast(msg, peer);
            }

            // ── gossip join ───────────────────────────────────────────────
            case MessageType.GOSSIP_JOIN -> {
                peer.setRemoteNodeId(msg.originalId);
                membership.handleJoin(msg.payload);
                // forward to others so the whole cluster learns about the joiner
                msg.ttl--;
                node.broadcast(msg, peer);
            }

            // ── gossip sync (membership view OR store anti-entropy) ───────
            case MessageType.GOSSIP_SYNC -> {
                if (msg.payload.startsWith("__STORE_SYNC__:")) {
                    store.mergeRemoteStore(msg.payload);
                } else {
                    membership.mergeGossip(msg.payload);
                }
                // GOSSIP_SYNC is not re-broadcast; each node picks its own targets
            }

            // ── heartbeat ─────────────────────────────────────────────────
            case MessageType.HEARTBEAT -> {
                peer.setRemoteNodeId(msg.originalId);
                long counter = parseLong(msg.payload);
                heartbeat.onHeartbeat(msg.originalId, counter);
                // heartbeats are NOT forwarded (TTL = 1)
            }

            case MessageType.HEARTBEAT_ACK -> {
                long counter = parseLong(msg.payload);
                heartbeat.onHeartbeatAck(msg.originalId, counter);
            }

            // ── store put ─────────────────────────────────────────────────
            case MessageType.STORE_PUT -> {
                store.applyRemotePut(msg.key, msg.value);
                // forward so all nodes replicate
                msg.ttl--;
                node.broadcast(msg, peer);
            }

            default -> System.err.println("[Handler] Unknown message type: " + msg.type);
        }
    }

    private long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}