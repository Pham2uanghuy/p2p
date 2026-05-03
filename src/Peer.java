import message.Message;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;


public class Peer {
    private final Socket socket;
    private final DataOutputStream outputStream;

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
            e.printStackTrace();
        }
    }

    public Socket getSocket() {
        return socket;
    }
}