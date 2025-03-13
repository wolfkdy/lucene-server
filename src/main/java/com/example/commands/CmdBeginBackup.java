package com.example.commands;

import com.example.protocol.MongoMessage;
import com.example.server.MongoServer;
import com.example.storage.IndexAccess;
import com.example.storage.IndexCatalog;
import com.example.storage.WriteBatch;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.Collection;

public class CmdBeginBackup extends Command {
    private static final Logger log = LogManager.getLogger(CmdBeginBackup.class);

    public CmdBeginBackup() {
        super("beginBackup");
    }

    /*
     * {
     *     "beginBackup": 1,
     * }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        IndexCatalog ic = MongoServer.getInstance().getIndexCatalog();
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        // TODO: should we do a manual commit first?
        Collection<String> indexNames = ic.beginBackup();
        writer.writeStartArray("files");
        for (String s : indexNames) {
            writer.writeString(s);
        }
        writer.writeEndArray();
        writer.writeEndDocument();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
