package com.luceneserver.commands.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.server.mongo.MongoServer;
import com.luceneserver.storage.HnswIndexAccess;
import com.luceneserver.storage.IndexAccess;
import com.luceneserver.storage.WriteBatch;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexOptions;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class CmdBatchWrite extends Command {
    private static final Logger log = LogManager.getLogger(CmdBatchWrite.class);

    public CmdBatchWrite() {
        super("batchWrite");
    }

    private WriteBatch.Op parseHNSWOp(BsonDocument d) {
        final WriteBatch.HnswOp op = new WriteBatch.HnswOp();
        List<BsonValue> bsonVec = d.getArray("vector", null);
        if (bsonVec != null) {
            float[] vec = new float[bsonVec.size()];
            int j = 0;
            for (BsonValue bf : bsonVec) {
                vec[j++] = (float)bf.asDouble().getValue();
            }
            op.vector = vec;
        }
        return op;
    }

    private WriteBatch.Op parseSearchOp(BsonDocument d) {
        final WriteBatch.SearchOp op = new WriteBatch.SearchOp();
        List<WriteBatch.SearchOp.Field> fields = new ArrayList<>();
        d.forEach((fname, fval) -> {
            if (fname.equals("op") || fname.equals("id")) {
                return;
            }
            BsonDocument fdoc = fval.asDocument();
            BsonDocument options = fdoc.getDocument("options");
            WriteBatch.SearchOp.Field field = new WriteBatch.SearchOp.Field();
            field.fieldName = fname;
            field.text = fdoc.getString("data").getValue();
            String indexOptions = options.getString("indexOptions").getValue();
            if (indexOptions.equals("offsets")) {
                field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
            } else if (indexOptions.equals("positions")) {
                field.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
            } else if (indexOptions.equals("freqs")) {
                field.indexOptions = IndexOptions.DOCS_AND_FREQS;
            } else if (indexOptions.equals("docs")) {
                field.indexOptions = IndexOptions.DOCS;
            } else {
                throw new IllegalArgumentException("invalid indexOptions " + indexOptions);
            }
            String norms = options.getString("norms").getValue();
            if (norms.equals("include")) {
                field.norms = true;
            } else if (norms.equals("omit")) {
                field.norms = false;
            } else {
                throw new IllegalArgumentException("invalid norms " + norms);
            }
            field.store = options.getBoolean("store").getValue();
            fields.add(field);
        });
        op.fields = fields.toArray(new WriteBatch.SearchOp.Field[0]);
        return op;
    }

    public WriteBatch decodeRequest(IndexAccess ia, RawBsonDocument bson) {
        String indexName = bson.getString("indexName").getValue();
        List<BsonValue> bvs = bson.getArray("ops").getValues();
        WriteBatch.Op[] opArr = new WriteBatch.Op[bvs.size()];
        int i = 0;
        for (BsonValue v : bvs) {
            final WriteBatch.Op op;
            BsonDocument d = v.asDocument();
            if (ia instanceof HnswIndexAccess) {
                op = parseHNSWOp(d);
            } else {
                op = parseSearchOp(d);
            }
            op.op = d.getString("op").getValue();
            op.id = d.getString("id").getValue();
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
        String indexName = msg.getBody().getString("indexName").getValue();
        IndexAccess ia = MongoServer.getInstance().getIndexCatalog().getIndex(indexName);
        if (ia == null) {
            return Command.createErrRspWithMsg("index not exists");
        }
        final WriteBatch batch = decodeRequest(ia, msg.getBody());

        ia.batchWrite(batch);
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeInt32("ok", 1);
        writer.writeEndDocument();
        return new RawBsonDocument(outputBuffer.toByteArray());
    }
}
