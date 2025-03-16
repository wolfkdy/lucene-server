package com.luceneserver.protocol.mongo;

import org.bson.RawBsonDocument;

public class MongoMessage {

    public static final int SECTION_KIND_BODY = 0;
    public static final int SECTION_KIND_DOCUMENT_SEQUENCE = 1;

    private final RawBsonDocument body;
    private final MsgHeader header;
    private final int flags = 0;
    private final boolean legacyFormat;

    public MongoMessage(MsgHeader header, RawBsonDocument body, boolean legacyFormat) {
        this.body = body;
        this.header = header;
        this.legacyFormat = legacyFormat;
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

    public boolean isLegacyFormat() {
        return legacyFormat;
    }
}