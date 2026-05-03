import member.MemberInfo;
import message.Message;
import message.MessageType;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class ConnectionHandler implements Runnable {
    private static final Set<String> seenMessages = ConcurrentHashMap.newKeySet();
    private final Peer peer;
    private final Node node;

    public ConnectionHandler(Peer peer, Node node) {
        this.peer = peer;
        this.node = node;
    }

    @Override
    public void run() {
        try (DataInputStream in =
                     new DataInputStream(peer.getSocket().getInputStream())) {

            while (true) {
                int length = in.readInt();
                byte[] data = new byte[length];
                in.readFully(data);

                Message msg = Message.fromBytes(data);

                // 1. version check
                // TODO: still hardcode, need to fix this
                if (msg.version != 1) {
                    System.out.println("Unsupported version: " + msg.version);
                    continue;
                }

                // 2. TTL check
                msg.ttl--;
                if (msg.ttl <= 0) continue;


                // 3. dedup
                if (seenMessages.contains(msg.id)) {
                    continue;
                }
                seenMessages.add(msg.id);

                // 4. decrease ttl before forwarding msg
                switch (msg.type) {
                    case MessageType.CHAT -> handleChat(msg);
                    case MessageType.GOSSIP -> handleGossip(msg);
                    default -> {
                        System.out.println("Unknow message type: " + msg.type);
                        continue;
                    }
                }


                // 5. broadcast msg to other nodes
                for (Peer p : node.getPeers()) {
                    if (p != peer) {
                        p.send(msg);
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("Peer disconnected");
            node.removePeer(peer);
        }
    }

    private void handleChat(Message msg) {
        System.out.println(msg.toString());
    }

    private void handleGossip(Message msg) {
        Map<String, MemberInfo> incoming = Message.decodeMembership(msg.payload);

        for (var entry: incoming.entrySet()) {
            String nodeId = entry.getKey();
            MemberInfo remote = entry.getValue();

            MemberInfo local = node.getMembership().get(nodeId);

            // case 1: unknown node
            if (local == null) {
                node.getMembership().put(nodeId, remote);
                continue;
            }

            // case 2: compare heart beat
            if (remote.getHeartbeat() > local.getHeartbeat()) {
                node.getMembership().put(nodeId, remote);
            }

        }
    }
}