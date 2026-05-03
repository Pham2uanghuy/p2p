import message.Message;
import message.MessageType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * P2P node with:
 * • Gossip membership  (MembershipManager)
 * • Heartbeat + failure detection (HeartbeatManager)
 * • Eventually-consistent KV store (KVStore)
 * <p>
 * CLI commands (type in console):
 * /connect <host> <port>   – connect to a peer
 * /members                 – print membership view
 * /store                   – print local KV store
 * /put <key> <value>       – write a key
 * /get <key>               – read a key locally
 * anything else            – send as CHAT message
 */
public class Node {

    //  identity 
    private final String nodeId;
    private final String host;
    private final int port;

    //  peer list 
    private final List<Peer> peers = new CopyOnWriteArrayList<>();

    //  subsystems 
    private final MembershipManager membershipManager;
    private final HeartbeatManager heartbeatManager;
    private final KVStore kvStore;

    //  constructor 

    public Node(String host, int port) {
        this.nodeId = UUID.randomUUID().toString();
        this.host = host;
        this.port = port;

        //  GossipSender adapter 
        MembershipManager.GossipSender gossipSender = new MembershipManager.GossipSender() {
            @Override
            public void broadcast(Message msg) {
                Node.this.broadcast(msg, null);
            }

            @Override
            public List<String> getPeerNodeIds() {
                return Node.this.getPeerNodeIds();
            }

            @Override
            public void sendTo(String nodeId, Message msg) {
                Node.this.sendTo(nodeId, msg);
            }
        };

        membershipManager = new MembershipManager(nodeId, host, port, gossipSender);

        //  HeartbeatSender adapter 
        HeartbeatManager.HbSender hbSender = new HeartbeatManager.HbSender() {
            @Override
            public void broadcast(Message msg) {
                Node.this.broadcast(msg, null);
            }

            @Override
            public List<String> getPeerNodeIds() {
                return Node.this.getPeerNodeIds();
            }

            @Override
            public void sendTo(String nodeId, Message msg) {
                Node.this.sendTo(nodeId, msg);
            }
        };

        heartbeatManager = new HeartbeatManager(nodeId, membershipManager, hbSender);

        //  StoreSender adapter 
        KVStore.StoreSender storeSender = new KVStore.StoreSender() {
            @Override
            public void broadcast(Message msg) {
                Node.this.broadcast(msg, null);
            }

            @Override
            public List<String> getPeerNodeIds() {
                return Node.this.getPeerNodeIds();
            }

            @Override
            public void sendTo(String nodeId, Message msg) {
                Node.this.sendTo(nodeId, msg);
            }
        };

        kvStore = new KVStore(nodeId, storeSender);
    }

    //  main 

    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 5000;

        if (args.length >= 1) port = Integer.parseInt(args[0]);
        if (args.length >= 2) host = args[1];

        Node node = new Node(host, port);

        // Connect to a seed peer if provided
        if (args.length >= 4) {
            node.connect(args[2], Integer.parseInt(args[3]));
        }

        node.start();
    }

    //  peer management 

    private void addPeer(Socket socket) throws IOException {
        Peer peer = new Peer(socket);
        peers.add(peer);
        new Thread(new ConnectionHandler(peer, this)).start();
    }

    public void removePeer(Peer peer) {
        peers.remove(peer);
    }

    /**
     * Broadcast to all peers, optionally skipping `exclude` (the inbound peer,
     * to avoid sending a message back to its source).
     */
    public void broadcast(Message message, Peer exclude) {
        for (Peer p : peers) {
            if (p != exclude) p.send(message);
        }
    }

    /**
     * Send a message to the peer identified by nodeId.
     */
    public void sendTo(String targetNodeId, Message message) {
        peers.stream()
                .filter(p -> targetNodeId.equals(p.getRemoteNodeId()))
                .findFirst()
                .ifPresent(p -> p.send(message));
    }

    public List<String> getPeerNodeIds() {
        return peers.stream()
                .map(Peer::getRemoteNodeId)
                .filter(id -> id != null)
                .collect(Collectors.toList());
    }

    //  connect to remote 

    public void connect(String remoteHost, int remotePort) throws IOException {
        Socket socket = new Socket(remoteHost, remotePort);
        addPeer(socket);

        // Announce ourselves so the remote node learns our nodeId + listen port
        Peer newPeer = peers.get(peers.size() - 1);
        newPeer.send(membershipManager.buildJoinMessage());
        System.out.printf("[Node] Connected to %s:%d and sent JOIN%n", remoteHost, remotePort);
    }

    //  start 

    public void start() throws IOException {
        System.out.printf("[Node] Starting node %s on port %d%n", nodeId, port);

        // Start subsystems
        membershipManager.start();
        heartbeatManager.start();
        kvStore.start();

        // Accept inbound connections
        ServerSocket serverSocket = new ServerSocket(port);
        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    addPeer(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // CLI loop
        System.out.println("[Node] Ready. Commands: /connect <host> <port> | /members | /store | /put <k> <v> | /get <k>");
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line = console.readLine();
            if (line == null) break;
            handleCommand(line.trim());
        }
    }

    //  CLI 

    private void handleCommand(String line) {
        if (line.startsWith("/connect ")) {
            String[] parts = line.split(" ");
            try {
                connect(parts[1], Integer.parseInt(parts[2]));
            } catch (Exception e) {
                System.err.println("Usage: /connect <host> <port>");
            }

        } else if (line.equals("/members")) {
            membershipManager.printView();

        } else if (line.equals("/store")) {
            kvStore.printStore();

        } else if (line.startsWith("/put ")) {
            String[] parts = line.split(" ", 3);
            if (parts.length < 3) {
                System.err.println("Usage: /put <key> <value>");
                return;
            }
            kvStore.put(parts[1], parts[2]);

        } else if (line.startsWith("/get ")) {
            String[] parts = line.split(" ", 2);
            String key = parts[1];
            Optional<VersionedValue> result = kvStore.get(key);
            result.ifPresentOrElse(
                    vv -> System.out.printf("[KVStore] GET %s = %s (ts=%d, writer=%s)%n",
                            key, vv.value, vv.timestamp, vv.writerNodeId),
                    () -> System.out.printf("[KVStore] GET %s → NOT FOUND%n", key)
            );

        } else if (!line.isEmpty()) {
            // Default: send as CHAT
            Message msg = new Message(
                    (byte) 1,
                    MessageType.CHAT,
                    (byte) 5,
                    nodeId,
                    line
            );
            broadcast(msg, null);
        }
    }

    //  getters for subsystems 

    public MembershipManager getMembershipManager() {
        return membershipManager;
    }

    public HeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    public KVStore getKVStore() {
        return kvStore;
    }

    public List<Peer> getPeers() {
        return peers;
    }

    public String getNodeId() {
        return nodeId;
    }
}