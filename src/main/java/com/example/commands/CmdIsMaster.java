package com.example.commands;

import com.example.protocol.MongoMessage;
import com.example.server.MessageProcessor;
import com.example.server.MongoServer;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.time.Instant;

import static com.example.transport.TransportConstants.*;

public class CmdIsMaster extends Command {
    private static final Logger log = LogManager.getLogger(CmdIsMaster.class);
    public CmdIsMaster() {
        super("isMaster", "ismaster");
    }
    public RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        writer.writeStartDocument();
        writer.writeBoolean("ismaster", true);
        writer.writeInt32("ok", 1);
        writer.writeInt32("maxBsonObjectSize", MAX_BSON_OBJECT_SIZE);
        writer.writeInt32("maxWriteBatchSize", MAX_WRITE_BATCH_SIZE);
        writer.writeInt32("maxMessageSizeBytes", MAX_MESSAGE_SIZE_BYTES);
        writer.writeInt32("maxWireVersion", MAX_WIRE_VERSION);
        writer.writeInt32("minWireVersion", MIN_WIRE_VERSION);
        writer.writeDateTime("localTime", Instant.now(MongoServer.getInstance().getClock()).toEpochMilli());
        writer.writeEndDocument();
        writer.close();
        RawBsonDocument rsp = new RawBsonDocument(outputBuffer.toByteArray());
        log.info("build ismaster resp {}", rsp);
        return rsp;
    }
}