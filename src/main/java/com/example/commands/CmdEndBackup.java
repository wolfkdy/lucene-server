package com.example.commands;

import com.example.protocol.MongoMessage;
import com.example.server.MongoServer;
import com.example.storage.IndexCatalog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.Collection;

public class CmdEndBackup extends Command {
    private static final Logger log = LogManager.getLogger(CmdEndBackup.class);

    public CmdEndBackup() {
        super("endBackup");
    }

    /*
     * {
     *     "endBackup": 1,
     * }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        IndexCatalog ic = MongoServer.getInstance().getIndexCatalog();
        ic.endBackup();
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeEndDocument();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }

}
