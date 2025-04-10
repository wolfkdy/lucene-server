package com.luceneserver.commands.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.server.mongo.MongoServer;
import com.luceneserver.storage.IndexCatalog;
import com.luceneserver.storage.SearchIndexAccess;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CmdCreateSearchIndex extends Command {
    private static final Logger log = LogManager.getLogger(CmdCreateSearchIndex.class);

    public CmdCreateSearchIndex() {
        super("createSearchIndex");
    }

    /*
     *  {
     *      "name": "testIndex",
     *  }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) {
        final String indexName, defaultAnalyzer;
        Map<String, String> perFieldAnalyzer = new HashMap<>();
        try {
            BsonDocument d = msg.getBody();
            indexName = d.getString("name").getValue();
            defaultAnalyzer = d.getString("defaultAnalyzer").getValue();
            BsonDocument perField  = d.getDocument("perFieldAnalyzer");
            perField.forEach((k, v) -> {
                perFieldAnalyzer.put(k, v.asString().getValue());
            });
        } catch (Exception e) {
            return Command.createErrRspWithMsg(e.getMessage());
        }
        IndexCatalog.IndexConfig cfg = new IndexCatalog.IndexConfig();
        cfg.name = indexName;
        cfg.path = indexName;
        cfg.searchConfig = new SearchIndexAccess.SearchConfig();
        cfg.searchConfig.defaultAnalyzer = defaultAnalyzer;
        cfg.searchConfig.perFieldAnalyzer = perFieldAnalyzer;
        try {
            MongoServer.getInstance().getIndexCatalog().createIndex(cfg);
        } catch (IOException e) {
            return Command.createErrRspWithMsg(e.getMessage());
        }
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeEndDocument();
        writer.close();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
