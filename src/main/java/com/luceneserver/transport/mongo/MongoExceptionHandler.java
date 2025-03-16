package com.luceneserver.transport.mongo;

import org.apache.logging.log4j.LogManager;


import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.logging.log4j.Logger;

public class MongoExceptionHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LogManager.getLogger(MongoExceptionHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("exception for client {}", ctx.channel().id(), cause);
        ctx.channel().close();
    }
}
