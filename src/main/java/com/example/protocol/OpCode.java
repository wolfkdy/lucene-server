package com.example.protocol;

import java.util.HashMap;
import java.util.Map;

public enum OpCode {
    OP_REPLY(1), // Reply to a client request. responseTo is set

    OP_QUERY(2004),  // legacy mongo command
    OP_MSG(2013); // Send a message using the format introduced in MongoDB 3.6

    private final int id;

    private static final Map<Integer, OpCode> byIdMap = new HashMap<>();

    static {
        for (final OpCode opCode : values()) {
            byIdMap.put(Integer.valueOf(opCode.id), opCode);
        }
    }

    OpCode(final int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static OpCode getById(int id) {
        return byIdMap.get(Integer.valueOf(id));
    }

}