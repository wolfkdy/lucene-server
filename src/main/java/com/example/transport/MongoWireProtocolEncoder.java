package com.example.transport;

import com.example.protocol.MongoMessage;
import com.example.protocol.OpCode;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MongoWireProtocolEncoder extends MessageToByteEncoder<MongoMessage> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolEncoder.class);
    public MongoWireProtocolEncoder() {
        super();
    }
    @Override
    protected void encode(ChannelHandlerContext ctx, MongoMessage message, ByteBuf buf) {
        buf.writeIntLE(0); // write length later

        buf.writeIntLE(message.getHeader().getRequestID());
        buf.writeIntLE(message.getHeader().getResponseTo());
        buf.writeIntLE(OpCode.OP_MSG.getId());

        buf.writeIntLE(message.getFlags());
        buf.writeByte(MongoMessage.SECTION_KIND_BODY);

        RawBsonDocument document = message.getBody();
        try {
            byte []bodyBytes = document.getByteBuffer().array();
            buf.writeBytes(bodyBytes);
        } catch (RuntimeException e) {
            log.error("Failed to encode {}", document, e);
            ctx.channel().close();
            throw e;
        }

        log.debug("wrote message: {}", message);

        // now set the length
        int writerIndex = buf.writerIndex();
        buf.setIntLE(0, writerIndex);
    }
}