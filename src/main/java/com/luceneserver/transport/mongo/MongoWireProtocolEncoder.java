package com.luceneserver.transport.mongo;

import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.protocol.mongo.OpCode;
import com.luceneserver.storage.WriteBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.RawBsonDocument;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MongoWireProtocolEncoder extends MessageToByteEncoder<MongoMessage> {
    private static final Logger log = LogManager.getLogger(MongoWireProtocolEncoder.class);

    public MongoWireProtocolEncoder() {
        super();
    }
    @Override
    protected void encode(ChannelHandlerContext ctx, MongoMessage message, ByteBuf buf) {
        buf.writeIntLE(0); // write length later

        buf.writeIntLE(message.getHeader().getRequestID());
        buf.writeIntLE(message.getHeader().getResponseTo());
        if (message.isLegacyFormat()) {
            buf.writeIntLE(OpCode.OP_REPLY.getId());
        } else {
            buf.writeIntLE(OpCode.OP_MSG.getId());
        }
        buf.writeIntLE(message.getFlags());
        if (message.isLegacyFormat()) {
            buf.writeLongLE(0L); // cursorId
            buf.writeIntLE(0); // startFrom
            buf.writeIntLE(1); // body message count
        } else {
            buf.writeByte(MongoMessage.SECTION_KIND_BODY);
        }

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