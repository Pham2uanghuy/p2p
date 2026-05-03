import member.MemberInfo;
import message.Message;
import message.MessageType;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ConnectionHandler implements Runnable {
    private static final Set<String> seenMessages = new HashSet<>();
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
                if (msg.ttl <= 0) continue;


                // 3. dedup
                if (seenMessages.contains(msg.id)) {
                    continue;
                }
                seenMessages.add(msg.id);

                System.out.println(msg.toString());

                // 4. decrease ttl before forwarding msg
                switch (msg.type) {
                    case MessageType.CHAT -> handleChat(msg);
                    case MessageType.GOSSIP -> handleGossip(msg);
                    default -> {
                        System.out.println("Unknow message type: " + msg.type);
                        continue;
                    }
                }
                msg.ttl--;

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
        Map<String, MemberInfo> incoming = msg.payload;
    }
}