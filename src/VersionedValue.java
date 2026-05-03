

/**
 * A value stored in the KV store, tagged with a Lamport timestamp so that
 * concurrent writes from different nodes can be reconciled deterministically
 * (last-write-wins by timestamp; node-id breaks ties).
 */
public class VersionedValue {

    public final String value;
    public final long   timestamp;   // System.currentTimeMillis() at write time
    public final String writerNodeId; // for tie-breaking

    public VersionedValue(String value, long timestamp, String writerNodeId) {
        this.value        = value;
        this.timestamp    = timestamp;
        this.writerNodeId = writerNodeId;
    }

    /**
     * Returns true if `this` should win over `other`
     * (higher timestamp, or same timestamp but lexicographically larger nodeId).
     */
    public boolean winsOver(VersionedValue other) {
        if (other == null) return true;
        if (this.timestamp != other.timestamp) return this.timestamp > other.timestamp;
        return this.writerNodeId.compareTo(other.writerNodeId) > 0;
    }

    /** Compact serialisation for embedding in a gossip/PUT message payload. */
    public String serialize() {
        return timestamp + "|" + writerNodeId + "|" + value;
    }

    public static VersionedValue deserialize(String s) {
        // format: timestamp|writerNodeId|value  (value may contain '|')
        int first  = s.indexOf('|');
        int second = s.indexOf('|', first + 1);
        long ts        = Long.parseLong(s.substring(0, first));
        String writer  = s.substring(first + 1, second);
        String val     = s.substring(second + 1);
        return new VersionedValue(val, ts, writer);
    }

    @Override
    public String toString() {
        return String.format("VersionedValue{value='%s', ts=%d, writer=%s}", value, timestamp, writerNodeId);
    }

}