package message;

import member.MemberInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Message {
    public byte version;
    public byte type;
    public byte ttl;
    public String id;          // messageId
    public String originalId;  // sender nodeId
    public long timestamp;
    public String payload;

    public Message(byte version, byte type, byte ttl, String originalId, String payload) {
        this.version = version;
        this.type = type;
        this.ttl = ttl;
        this.id = UUID.randomUUID().toString();
        this.originalId = originalId;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public static Message gossip(Map<String, MemberInfo> membership, String fromNodeId) {
        String payload = membership.toString();
        return new Message(
                (byte) 1, // version
                (byte) 2, // type
                (byte) 1, // ttl
                fromNodeId,
                payload
        );
    }

    public static Message fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);

        byte version = buf.get();
        byte type = buf.get();
        byte ttl = buf.get();
        long msgMost = buf.getLong();
        long msgLeast = buf.getLong();
        String messageId = new UUID(msgMost, msgLeast).toString();
        long origMost = buf.getLong();
        long origLeast = buf.getLong();
        String originalId = new UUID(origMost, origLeast).toString();
        long timestamp = buf.getLong();
        int payloadLen = buf.getInt();
        byte[] payloadBytes = new byte[payloadLen];
        buf.get(payloadBytes);
        String payload = new String(payloadBytes, StandardCharsets.UTF_8);

        Message msg = new Message(version, type, ttl, originalId, payload);
        msg.id = messageId;
        msg.timestamp = timestamp;
        return msg;
    }

    public byte[] toBytes() throws IOException {
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.allocate(
                1   // version
                        + 1   // type
                        + 1   // ttl
                        + 16  // messageId
                        + 16  // originalId
                        + 8   // timestamp
                        + 4   // payload length
                        + payloadBytes.length
        );

        buffer.put(version);
        buffer.put(type);
        buffer.put(ttl);
        // messageId
        UUID msgUUID = UUID.fromString(id);
        buffer.putLong(msgUUID.getMostSignificantBits());
        buffer.putLong(msgUUID.getLeastSignificantBits());
        // originalId
        UUID originalUUID = UUID.fromString(originalId);
        buffer.putLong(originalUUID.getMostSignificantBits());
        buffer.putLong(originalUUID.getLeastSignificantBits());
        buffer.putLong(timestamp);
        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);

        return buffer.array();
    }

    public static byte[] encodeMembership(Map<String, MemberInfo> membership) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        buffer.putInt(membership.size());

        for (MemberInfo memberInfo : membership.values()) {
            UUID uuid = UUID.fromString(memberInfo.getNodeId());
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            buffer.putLong(memberInfo.getHeartbeat());
            buffer.putLong(memberInfo.getLastUpdated());
            buffer.put(memberInfo.getStatus());
            byte[] hostBytes = memberInfo.getHost().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(hostBytes.length);
            buffer.put(hostBytes);
            buffer.putInt(memberInfo.getPort());
        }

        buffer.flip();
        byte[] result = new byte[buffer.limit()];
        buffer.get(result);
        return result;
    }

    public static Map<String, MemberInfo> decodeMembership(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        Map<String, MemberInfo> map = new HashMap<>();

        int size = buf.getInt();

        for (int i = 0; i < size; i++) {
            long most = buf.getLong();
            long least = buf.getLong();
            String nodeId = new UUID(most, least).toString();
            long heartbeat = buf.getLong();
            long lastUpdated = buf.getLong();
            byte status = buf.get();
            int hostLen = buf.getInt();
            byte[] hostBytes = new byte[hostLen];
            buf.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);
            int port = buf.getInt();
            MemberInfo m = new MemberInfo();
            m.setNodeId(nodeId);
            m.setHeartbeat(heartbeat);
            m.setLastUpdated(lastUpdated);
            m.setStatus(status);
            m.setHost(host);
            m.setPort(port);

            map.put(nodeId, m);
        }

        return map;
    }

    @Override
    public String toString() {
        return String.format("Message{type=%d, id=%s, from=%s, ttl=%d, payload='%s'}",
                type, id, originalId, ttl, payload);
    }
}