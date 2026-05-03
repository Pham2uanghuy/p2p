import java.io.IOException;

public class NodeRunner2 {
    public static void main(String[] args) throws IOException {
        int port = 5002;
        Node node1 = new Node(port);
        node1.connect("localhost", 5000);
        node1.start();
    }
}
