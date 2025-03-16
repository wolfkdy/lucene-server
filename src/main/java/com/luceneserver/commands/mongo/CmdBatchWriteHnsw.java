package com.luceneserver.commands.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.server.mongo.MongoServer;
import com.luceneserver.storage.IndexAccess;
import com.luceneserver.storage.WriteBatch;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class CmdBatchWriteHnsw extends Command {
    private static final Logger log = LogManager.getLogger(CmdBatchWriteHnsw.class);

    public CmdBatchWriteHnsw() {
        super("batchWriteHnsw");
    }

    public WriteBatch decodeRequest(RawBsonDocument bson) {
        String indexName = bson.getString("indexName").getValue();
        List<BsonValue> bvs = bson.getArray("ops").getValues();
        WriteBatch.Op[] opArr = new WriteBatch.Op[bvs.size()];
        int i = 0;
        for (BsonValue v : bvs) {
            final WriteBatch.HnswOp op = new WriteBatch.HnswOp();
            BsonDocument d = v.asDocument();
            op.op = d.getString("op").getValue();
            op.id = d.getString("id").getValue();
            List<BsonValue> bsonVec = d.getArray("vector", null);
            if (bsonVec != null) {
                float[] vec = new float[bsonVec.size()];
                int j = 0;
                for (BsonValue bf : bsonVec) {
                    vec[j++] = (float)bf.asDouble().getValue();
                }
                op.vector = vec;
            }
            opArr[i++] = op;
        }
        boolean autoCommit = bson.getBoolean("autoCommit").getValue();
        WriteBatch result = new WriteBatch();
        result.autoCommit = autoCommit;
        result.ops = opArr;
        result.indexName = indexName;
        return result;
    }

    /*
     * {
     *     "batchWrite": 1,
     *     "indexName": indexName,
     *     "ops": [
     *        {op: "i|u|d", id: String, field0: String, field1: String, ...}
     *     ],
     *     "autoCommit": true|false
     * }
     */
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        final WriteBatch batch = decodeRequest(msg.getBody());
        IndexAccess ia = MongoServer.getInstance().getIndexCatalog().getIndex(batch.indexName);
        if (ia == null) {
            return Command.createErrRspWithMsg("index not exists");
        }
        ia.batchWrite(batch);
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeEndDocument();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
