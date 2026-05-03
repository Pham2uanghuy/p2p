package message;

public class MessageType {
    public static final byte CHAT = 1;
    public static final byte GOSSIP = 2;  // node announces itself
    public static final byte GOSSIP_SYNC = 3;  // membership list sync
    public static final byte HEARTBEAT = 4;  // liveness ping
    public static final byte HEARTBEAT_ACK = 5;
    public static final byte STORE_PUT = 6;  // key-value write
    public static final byte STORE_GET = 7;  // key-value read request
    public static final byte STORE_GET_RESP = 8;
}
