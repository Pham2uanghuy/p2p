import java.io.IOException;

public class NodeRunner1 {
    public static void main(String[] args) throws IOException {
        int port = 5001;
        Node node1 = new Node(port);
        node1.connect("localhost", 5000);
        node1.start();
    }
}
