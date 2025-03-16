package com.luceneserver.commands.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.server.mongo.MongoServer;
import com.luceneserver.storage.IndexCatalog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.Set;

public class CmdListIndexes extends Command{
    private static final Logger log = LogManager.getLogger(CmdListIndexes.class);

    public CmdListIndexes() {
        super("listIndexes");
    }

    /*
     * {
     *     "listIndexes": 1,
     *     "nameOnly: false
     * }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        BsonDocument doc = msg.getBody();
        boolean nameOnly = doc.getBoolean("nameOnly").getValue();
        IndexCatalog ic = MongoServer.getInstance().getIndexCatalog();
        Set<String> indexNames = ic.getAllIndexNames();

        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeTimestamp("committedTimestamp", new BsonTimestamp(ic.getCommittedTimestamp()));
        writer.writeStartArray("indexes");
        for (String name : indexNames) {
            writer.writeStartDocument();
            writer.writeString("name", name);
            writer.writeEndDocument();
        }
        writer.writeEndArray();
        writer.writeInt32("ok", 1);
        writer.writeEndDocument();
        writer.close();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
