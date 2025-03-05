package com.example.server;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.protocol.MongoMessage;
import com.example.protocol.MsgHeader;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;

import static com.example.protocol.MongoWireProtocolHandler.MAX_BSON_OBJECT_SIZE;

public class MongoServerHandler extends SimpleChannelInboundHandler<MongoMessage> {
    private static final Logger log = LoggerFactory.getLogger(MongoServerHandler.class);

    private final ChannelGroup channelGroup;
    private final AtomicInteger msgIdSeq = new AtomicInteger();

    public static final int MAX_WRITE_BATCH_SIZE = 10000;
    public static final int MAX_MESSAGE_SIZE_BYTES = 48000000;
    public static final int MAX_WIRE_VERSION = 8;
    public static final int MIN_WIRE_VERSION = 0;

    public MongoServerHandler(ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channelGroup.add(ctx.channel());
        log.info("client {} connected", ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("channel {} closed", ctx.channel());
        channelGroup.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MongoMessage msg) {
        MongoMessage rsp = null;
        log.info("get cmd name {}", msg.getCommandName());
        if (msg.getCommandName().equals("ismaster") || msg.getCommandName().equals("isMaster")) {
            rsp = handleIsMaster(ctx, msg);
        } else {
            rsp = msg;
        }
        ctx.channel().writeAndFlush(rsp);
    }

    protected MsgHeader createResponseMsgHeader(MongoMessage inMsg) {
        return new MsgHeader(0, msgIdSeq.incrementAndGet(), inMsg.getHeader().getRequestID());
    }

    MongoMessage handleIsMaster(ChannelHandlerContext ctx, MongoMessage msg) {
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
        // TODO(deyukong): server time source
        writer.writeDateTime("localTime", Instant.now(Clock.systemDefaultZone()).getLong());
        writer.writeEndDocument();
        writer.close();
        RawBsonDocument body = new RawBsonDocument(outputBuffer.toByteArray());
        log.info("build ismaster resp {}", body);
        return new MongoMessage(ctx.channel(), createResponseMsgHeader(msg), body);
    }
}
