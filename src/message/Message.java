package message;

import member.MemberInfo;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
    public byte[] payload;

    public Message(byte version, byte type, byte ttl, String originalId, byte[] payload) {
        this.version = version;
        this.type = type;
        this.ttl = ttl;
        this.id = UUID.randomUUID().toString();
        this.originalId = originalId;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public static Message gossip(Map<String, MemberInfo> membership, String fromNodeId) {
        byte[] payload = encodeMembership(membership);
        return new Message(
                (byte) 1, // version
                (byte) 2, // type
                (byte) 3, // ttl
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

        Message msg = new Message(version, type, ttl, originalId, payloadBytes);
        msg.id = messageId;
        msg.timestamp = timestamp;
        return msg;
    }

    public byte[] toBytes() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(
                1   // version
                        + 1   // type
                        + 1   // ttl
                        + 16  // messageId
                        + 16  // originalId
                        + 8   // timestamp
                        + 4   // payload length
                        + payload.length
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
        buffer.putInt(payload.length);
        buffer.put(payload);

        return buffer.array();
    }

    public static byte[] encodeMembership(Map<String, MemberInfo> membership) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            out.writeInt(membership.size());
            for (MemberInfo memberInfo : membership.values()) {
                UUID uuid = UUID.fromString(memberInfo.getNodeId());
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());
                out.writeLong(memberInfo.getHeartbeat());
                out.writeLong(memberInfo.getLastUpdated());
                out.writeByte(memberInfo.getStatus());
                byte[] hostBytes = memberInfo.getHost().getBytes(StandardCharsets.UTF_8);
                out.writeInt(hostBytes.length);
                out.write(hostBytes);
                out.writeInt(memberInfo.getPort());
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        String payloadInString = new String(payload, StandardCharsets.UTF_8);
        return String.format("Message{type=%d, id=%s, from=%s, ttl=%d, payload='%s'}",
                type, id, originalId, ttl, payloadInString);
    }
}