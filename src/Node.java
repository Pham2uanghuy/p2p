import member.MemberInfo;
import member.Status;
import message.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Node {
    private final String nodeId;
    private final int port;
    private final List<Peer> peers = new CopyOnWriteArrayList<>();
    private final ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, MemberInfo> membership = new ConcurrentHashMap<>();


    public Node(int port) {
        this.nodeId = UUID.randomUUID().toString();
        this.port = port;
        MemberInfo self = new MemberInfo(nodeId, "localhost", port);
        membership.put(nodeId, self);
    }

    public static void main(String[] args) throws IOException {
        int port = 5000;
        Node node = new Node(port);

        node.start();
    }

    public void connect(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        addPeer(socket);
    }

    private void addPeer(Socket socket) throws IOException {
        Peer peer = new Peer(socket);
        peers.add(peer);
        connections.submit(new ConnectionHandler(peer, this));
    }

    public void removePeer(Peer peer) {
        peers.remove(peer);
    }

    void broadcast(Message message) {
        for (Peer p : peers) {
            p.send(message);
        }
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);

        Thread.startVirtualThread(() -> {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    addPeer(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread.startVirtualThread(() -> {
            try {
                while (true) {
                    MemberInfo self = membership.get(nodeId);
                    long newHearbeat = self.getHeartbeat() + 1;
                    self.setHeartbeat(newHearbeat);
                    self.setHeartbeat(System.currentTimeMillis());

                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });

        Thread.startVirtualThread(() -> {
            try {
                while (true) {
                    Message gossipMsg = Message.gossip(membership, this.nodeId);
                    if (peers.size() >= 3) {
                        List<Peer> randomPeers = pickRandom(2);

                        for (Peer p : randomPeers) {
                            p.send(gossipMsg);
                        }
                    } else {
                        for (Peer p : peers) {
                            p.send(gossipMsg);
                        }
                    }
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread.startVirtualThread(() -> {
            try {
                while (true) {
                    long now = System.currentTimeMillis();

                    for (MemberInfo m : membership.values()) {
                        if (this.nodeId.equals(m.getNodeId())) continue;

                        long diff = now - m.getLastUpdated();

                        if (diff > 5000) {
                            m.setStatus(Status.SUSPECT);
                        }
                        if (diff > 10000) {
                            m.setStatus(Status.DEAD);
                        }
                    }
                    Thread.sleep(2000);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });


        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String[] input = console.readLine().split(" ");
            String prefix = input[0];

            if (prefix.equals("CHAT")) {
                String payload = input[1];
                Message msg = new Message(
                        (byte) 1,          // version
                        (byte) 1,          // type (CHAT)
                        (byte) 5,          // ttl
                        this.nodeId,      // originalId (sender node)
                        payload.getBytes()
                );
                broadcast(msg);
            } else if ("MEMBERS".equals(prefix)) {
                for (var entry: membership.entrySet()) {
                    MemberInfo m  = entry.getValue();
                    System.out.println("nodeId: " + m.getNodeId() + " - status: " + m.getStatus());
                }
            }
        }
    }


    public List<Peer> getPeers() {
        return peers;
    }

    public List<Peer> pickRandom(int number) {
        return new Random()
                .ints(0, peers.size())
                .distinct()
                .limit(number)
                .mapToObj(peers::get)
                .toList();
    }

    public Map<String, MemberInfo> getMembership() {
        return this.membership;
    }
}