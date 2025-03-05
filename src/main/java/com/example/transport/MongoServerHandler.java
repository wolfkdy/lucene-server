package com.example.transport;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.protocol.MongoMessage;
import com.example.protocol.MsgHeader;
import com.example.server.MessageProcessor;
import com.example.server.MongoServer;
import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;


public class MongoServerHandler extends SimpleChannelInboundHandler<MongoMessage> {
    private static final Logger log = LoggerFactory.getLogger(MongoServerHandler.class);

    private final ChannelGroup channelGroup;
    private final AtomicInteger msgIdSeq = new AtomicInteger();



    public MongoServerHandler(ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channelGroup.add(ctx.channel());
        log.info("client {} connected, {} clients in total", ctx.channel(), channelGroup.size());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("channel {} closed, {} clients in total", ctx.channel(), channelGroup.size());
        channelGroup.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MongoMessage msg) {
        MessageProcessor mp = MongoServer.getInstance().getMessageProcessor();
        RawBsonDocument rsp = mp.handleMessage(ctx, msg);
        MongoMessage rspMsg = new MongoMessage(createResponseMsgHeader(msg), rsp);
        ctx.channel().writeAndFlush(rspMsg);
    }

    protected MsgHeader createResponseMsgHeader(MongoMessage inMsg) {
        return new MsgHeader(0, msgIdSeq.incrementAndGet(), inMsg.getHeader().getRequestID());
    }
}
