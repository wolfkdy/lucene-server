package com.example.transport;

public class TransportConstants {
    public static final int MAX_BSON_OBJECT_SIZE = 16*1024*1024;
    public static final int MAX_BSON_OBJECT_SIZE_INTERNAL = MAX_BSON_OBJECT_SIZE + 16*1024;
    public static final int MAX_MESSAGE_SIZE_BYTES = 4 * MAX_BSON_OBJECT_SIZE_INTERNAL;
    public static final int MAX_WRITE_BATCH_SIZE = 10000;
    public static final int MAX_WIRE_VERSION = 8;
    public static final int MIN_WIRE_VERSION = 0;
}
