import message.Message;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Wraps a connected socket with a buffered output stream.
 * Optionally tracks the remote nodeId once the peer has announced itself
 * via GOSSIP_JOIN so we can target it by id.
 */
public class Peer {
    private final Socket socket;
    private final DataOutputStream outputStream;

    /**
     * Set once we receive/send the remote node's JOIN announcement.
     */
    private volatile String remoteNodeId;

    public Peer(Socket socket) throws IOException {
        this.socket = socket;
        this.outputStream = new DataOutputStream(socket.getOutputStream());
    }

    public void send(Message msg) {
        try {
            byte[] data = msg.toBytes();
            outputStream.writeInt(data.length);
            outputStream.write(data);
            outputStream.flush();
        } catch (IOException e) {
            // Silently fail; ConnectionHandler will detect the broken stream
            // and call node.removePeer()
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public String getRemoteNodeId() {
        return remoteNodeId;
    }

    public void setRemoteNodeId(String nodeId) {
        this.remoteNodeId = nodeId;
    }

    public String getRemoteHost() {
        return socket.getInetAddress().getHostAddress();
    }

    public int getRemotePort() {
        // remote *listening* port isn't the ephemeral port—
        // we learn it from the JOIN message payload instead.
        return socket.getPort();
    }
}