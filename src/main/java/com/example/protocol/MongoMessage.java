package com.example.protocol;

import io.netty.channel.Channel;
import org.bson.RawBsonDocument;

public class MongoMessage {

    public static final int SECTION_KIND_BODY = 0;
    public static final int SECTION_KIND_DOCUMENT_SEQUENCE = 1;

    private final RawBsonDocument body;
    private final MsgHeader header;
    private final int flags = 0;

    public MongoMessage(MsgHeader header, RawBsonDocument body) {
        this.body = body;
        this.header = header;
    }

    public int getFlags() {
        return flags;
    }

    public MsgHeader getHeader() {
        return header;
    }

    public String getCommandName() {
        return body.getFirstKey();
    }

    public RawBsonDocument getBody() {
        return body;
    }
}