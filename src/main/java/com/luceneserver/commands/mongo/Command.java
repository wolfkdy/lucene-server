package com.luceneserver.commands.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import io.netty.channel.ChannelHandlerContext;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.HashMap;

public abstract class Command {
    private final String name;
    private final String oldName;
    Command(String name) {
        this.name = name;
        this.oldName = null;
    }
    Command(String name, String oldName) {
        this.name = name;
        this.oldName = oldName;
    }
    public abstract RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException;
    public void register(HashMap<String, Command> m) {
        m.put(name, this);
        if (oldName != null) {
            m.put(oldName, this);
        }
    }
    public static RawBsonDocument createErrRspWithMsg(String msg) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 0);
        writer.writeString("errmsg", msg);
        writer.writeEndDocument();
        writer.close();
        RawBsonDocument rsp = new RawBsonDocument(outputBuffer.toByteArray());
        return rsp;
    }
}
