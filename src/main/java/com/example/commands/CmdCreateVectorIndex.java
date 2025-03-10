package com.example.commands;

import com.example.protocol.MongoMessage;
import com.example.server.MongoServer;
import com.example.storage.HnswIndexAccess;
import com.example.storage.IndexCatalog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.time.Instant;

import static com.example.transport.TransportConstants.*;

public class CmdCreateVectorIndex extends Command {
    private static final Logger log = LogManager.getLogger(CmdCreateVectorIndex.class);

    public CmdCreateVectorIndex() {
        super("createVectorIndex");
    }

    /*
     *  {
     *      "name": "testIndex",
     *      "dimensions": 1024，
     *      “similarity": "euclidean",
     *      "quantization": "none",
     *      "efConstruction": 10, // beamWidth in lucene, efConstruction in paper
     *      "mmax": 16
     *  }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) {
        final String indexName, similarity, quantization;
        final int dimensions, efConstruction, mmax;

        try {
            indexName = msg.getBody().getString("name").getValue();
            dimensions = msg.getBody().getInt32("dimensions").getValue();
            similarity = msg.getBody().getString("similarity").getValue();
            quantization = msg.getBody().getString("quantization").getValue();
            efConstruction = msg.getBody().getInt32("efConstruction").getValue();
            mmax = msg.getBody().getInt32("mmax").getValue();
        } catch (Exception e) {
            return Command.createErrRspWithMsg(e.getMessage());
        }
        HnswIndexAccess.HnswConfig hnswConfig = new HnswIndexAccess.HnswConfig();
        hnswConfig.dimensions = dimensions;
        hnswConfig.efConstruction = efConstruction;
        hnswConfig.maxConn = mmax;
        hnswConfig.similarity = similarity;
        IndexCatalog.IndexConfig cfg = new IndexCatalog.IndexConfig();
        cfg.hnswConfig = hnswConfig;
        cfg.name = indexName;
        cfg.path = indexName;
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
