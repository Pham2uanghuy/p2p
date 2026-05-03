package message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Message {
    public byte version;
    public byte type;
    public byte ttl;
    public String id;          // messageId
    public String originalId;  // sender nodeId
    public long timestamp;
    public String payload;
    public String key;
    public String value;

    public Message(byte version, byte type, byte ttl, String originalId, String payload) {
        this.version = version;
        this.type = type;
        this.ttl = ttl;
        this.id = UUID.randomUUID().toString();
        this.originalId = originalId;
        this.payload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    public Message(byte version, byte type, byte ttl, String originalId,
                   String payload, String key, String value) {
        this(version, type, ttl, originalId, payload);
        this.key = key != null ? key : "";
        this.value = value != null ? value : "";
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

        // key / value (may be absent in old-format messages → defaults to "")
        String key = "", value = "";
        if (buf.remaining() >= 4) {
            int keyLen = buf.getInt();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }
        if (buf.remaining() >= 4) {
            int valueLen = buf.getInt();
            byte[] valueBytes = new byte[valueLen];
            buf.get(valueBytes);
            value = new String(valueBytes, StandardCharsets.UTF_8);
        }

        Message msg = new Message(version, type, ttl, originalId, payload, key, value);
        msg.id = messageId;
        msg.timestamp = timestamp;
        return msg;
    }

    public byte[] toBytes() throws IOException {
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = key.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.allocate(
                1   // version
                        + 1   // type
                        + 1   // ttl
                        + 16  // messageId
                        + 16  // originalId
                        + 8   // timestamp
                        + 4   // payload length
                        + payloadBytes.length
                        + 4 + keyBytes.length
                        + 4 + valueBytes.length
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
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);

        return buffer.array();
    }

    @Override
    public String toString() {
        return String.format("Message{type=%d, id=%s, from=%s, ttl=%d, payload='%s', key='%s', value='%s'}",
                type, id, originalId, ttl, payload, key, value);
    }
}