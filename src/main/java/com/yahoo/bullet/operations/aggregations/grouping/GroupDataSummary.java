package com.yahoo.bullet.operations.aggregations.grouping;

import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.UpdatableSummary;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GroupDataSummary implements UpdatableSummary<GroupData> {
    public static final int INITIALIZED_POSITION = 0;
    public static final int SIZE_POSITION = Byte.BYTES;
    public static final int DATA_POSITION = SIZE_POSITION + Integer.BYTES;

    private boolean initialized = false;
    @Getter
    private GroupData data;

    @Override
    public void update(GroupData value) {
        if (!initialized) {
            data = value;
            return;
        }
        data.combine(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public GroupDataSummary copy() {
        Map<String, String> copiedGroups = new HashMap<>(data.groupFields);
        Set<GroupOperation> copiedOperations = data.metrics != null ? data.metrics.keySet() : Collections.emptySet();
        GroupData copiedData = new GroupData(copiedGroups, copiedOperations);

        GroupDataSummary copy = new GroupDataSummary();
        copy.initialized = initialized;
        copy.data = copiedData;
        return copy;
    }

    @Override
    public byte[] toByteArray() {
        byte[] groupData = SerializerDeserializer.toBytes(data);
        int length = groupData.length;
        // Create a new ByteBuffer to hold a byte, an integer and the data in bytes
        return ByteBuffer.allocate(DATA_POSITION + length)
                         .put((byte) (initialized ? 1 : 0))
                         .putInt(length)
                         .put(groupData, DATA_POSITION, length)
                         .array();
    }

    /**
     * Needed to deserialize an instance of this {@link GroupDataSummary} from a {@link Memory}.
     *
     * @param serializedSummary The serialized summary as a {@link Memory} object.
     * @return A {@link DeserializeResult} representing the deserialized summary.
     */
    public static DeserializeResult<GroupDataSummary> fromMemory(Memory serializedSummary) {
        byte initialized = serializedSummary.getByte(INITIALIZED_POSITION);
        int size = serializedSummary.getInt(SIZE_POSITION);

        byte[] data = new byte[size];
        serializedSummary.getByteArray(DATA_POSITION, data, 0, size);
        GroupData deserializedData = SerializerDeserializer.fromBytes(data);

        GroupDataSummary deserialized = new GroupDataSummary();
        deserialized.initialized = initialized != 0;
        deserialized.data = deserializedData;

        // Size read is the size of size and the byte in bytes (DATA_POSITION) plus the size of the data (size)
        return new DeserializeResult<>(deserialized, size + DATA_POSITION);
    }
}
