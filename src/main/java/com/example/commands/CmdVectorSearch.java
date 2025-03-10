package com.example.commands;

import com.example.protocol.MongoMessage;
import com.example.server.MongoServer;
import com.example.storage.HnswIndexAccess;
import com.example.storage.IndexAccess;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.List;

public class CmdVectorSearch extends Command {
    private static final Logger log = LogManager.getLogger(CmdVectorSearch.class);

    public CmdVectorSearch() {
        super("vectorSearch");
    }

    /*
     *  {
     *      vectorSearch: 1,
     *      indexName: "test",
     *      vector: [1.0,2.0],
     *      k: 2 //
     *      numCandidates: 1
     *  }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        RawBsonDocument d = msg.getBody();
        String indexName = d.getString("indexName").getValue();
        List<BsonValue> bsonVec = d.getArray("vector");
        float[] vec = new float[bsonVec.size()];
        int j = 0;
        for (BsonValue bf : bsonVec) {
            vec[j++] = (float)bf.asDouble().getValue();
        }
        int k = d.getInt32("k").getValue();
        int m = d.getInt32("numCandidates").getValue();
        HnswIndexAccess ia = (HnswIndexAccess)MongoServer.getInstance().getIndexCatalog().getIndex(indexName);
        if (ia == null) {
            return Command.createErrRspWithMsg("index not exists");
        }

        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeStartArray("hits");
        for (String id : ia.knn(vec, k, m)) {
            writer.writeStartDocument();
            writer.writeString("id", id);
            writer.writeEndDocument();
        }
        writer.writeEndArray();
        writer.writeEndDocument();
        writer.close();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
