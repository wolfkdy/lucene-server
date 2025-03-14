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

public class CmdDropIndex extends Command {
    private static final Logger log = LogManager.getLogger(CmdDropIndex.class);

    public CmdDropIndex() {
        super("dropIndex");
    }

    /*
     *  {
     *      "name": "testIndex",
     *  }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        String name = msg.getBody().getString("name").getValue();
        IndexCatalog ic = MongoServer.getInstance().getIndexCatalog();
        ic.dropIndex(name);
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeEndDocument();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
