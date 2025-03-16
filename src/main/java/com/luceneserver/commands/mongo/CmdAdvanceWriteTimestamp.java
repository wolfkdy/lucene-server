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

public class CmdAdvanceWriteTimestamp extends Command{
    private static final Logger log = LogManager.getLogger(CmdAdvanceWriteTimestamp.class);

    public CmdAdvanceWriteTimestamp() {
        super("advanceWriteTimestamp");
    }

    /*
     * {
     *     "advanceWriteTimestamp": 1,
     *     "timestamp": Timestamp(1,2)
     * }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        long writeTimestamp = msg.getBody().getTimestamp("timestamp").asTimestamp().getValue();
        IndexCatalog ic = MongoServer.getInstance().getIndexCatalog();
        ic.advanceWriteTimestamp(writeTimestamp);
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeEndDocument();
        writer.close();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
