package com.luceneserver.commands.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.server.mongo.MongoServer;
import com.luceneserver.storage.IndexCatalog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;

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
