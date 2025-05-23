package com.luceneserver.transport.mongo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Set;

import com.luceneserver.protocol.mongo.MessageFlag;
import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.protocol.mongo.MsgHeader;
import com.luceneserver.protocol.mongo.OpCode;
import com.luceneserver.utils.Assert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.RawBsonDocument;


import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.luceneserver.transport.mongo.TransportConstants.MAX_MESSAGE_SIZE_BYTES;

public class MongoWireProtocolHandler extends LengthFieldBasedFrameDecoder {

    private static final Logger log = LogManager.getLogger(MongoWireProtocolHandler.class);

    // MAX_MESSAGE_SIZE_BYTES is from message.h ConstView::valid()


    private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 4;
    private static final int LENGTH_ADJUSTMENT = -LENGTH_FIELD_LENGTH;
    private static final int INITIAL_BYTES_TO_STRIP = 0;
    private static final int CHECKSUM_LENGTH = 4;

    public MongoWireProtocolHandler() {
        super(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, INITIAL_BYTES_TO_STRIP);
    }

    @Override
    protected MongoMessage decode(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
        ByteBuf in = buf;

        if (in.readableBytes() < 4) {
            return null;
        }

        in.markReaderIndex();
        int totalLength = in.readIntLE();

        if (totalLength > MAX_MESSAGE_SIZE_BYTES) {
            throw new IOException("message too large: " + totalLength + " bytes");
        }

        if (in.readableBytes() < totalLength - LENGTH_FIELD_LENGTH) {
            in.resetReaderIndex();
            return null; // retry
        }
        in = in.readSlice(totalLength - LENGTH_FIELD_LENGTH);
        long readable = in.readableBytes();
        Assert.equals(readable, totalLength - LENGTH_FIELD_LENGTH);

        final int requestID = in.readIntLE();
        final int responseTo = in.readIntLE();
        final MsgHeader header = new MsgHeader(totalLength, requestID, responseTo);

        int opCodeId = in.readIntLE();
        OpCode opCode = OpCode.getById(opCodeId);
        if (opCode == null) {
            throw new IOException("opCode " + opCodeId + " not supported");
        }

        final Channel channel = ctx.channel();
        final MongoMessage request;

        switch (opCode) {
            case OP_MSG:
                request = handleMessage(channel, header, in);
                break;
            case OP_QUERY:
                request = handleLegacyMessage(channel, header, in);
                break;
            default:
                throw new UnsupportedOperationException("unsupported opcode: " + opCode);
        }

        if (in.isReadable()) {
            throw new IOException("channal still readable after handleMessage");
        }

        log.debug("get cmd {}", request.getBody());

        return request;
    }

    /*
     * see mongodb document, wire-legacy-opcodes part
     */
    private MongoMessage handleLegacyMessage(Channel channel, MsgHeader header, ByteBuf buffer) {
        int flags = buffer.readIntLE();

        int length = buffer.bytesBefore((byte)'\0');
        if (length < 0)
            throw new IllegalArgumentException("string termination not found");
        final String fullCollectionName = buffer.toString(buffer.readerIndex(), length, StandardCharsets.UTF_8);
        buffer.skipBytes(length + 1);

        final int numberToSkip = buffer.readIntLE();
        final int numberToReturn = buffer.readIntLE();

        RawBsonDocument query = null, returnFieldSector = null;
        while (buffer.isReadable()) {
            buffer.markReaderIndex();
            final int bsonLen = buffer.readIntLE();
            buffer.resetReaderIndex();

            byte[] bytes = new byte[bsonLen];
            buffer.readBytes(bytes);
            if (query == null) {
                query = new RawBsonDocument(bytes);
            } else if (returnFieldSector == null) {
                returnFieldSector = new RawBsonDocument(bytes);
            } else {
                throw new IllegalArgumentException("invalid legacy message format");
            }
        }

        if (flags != 0) {
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");
        }

        log.info("query {} from {}", query, fullCollectionName);

        return new MongoMessage(header, query, true);
    }
    private MongoMessage handleMessage(Channel channel, MsgHeader header, ByteBuf buffer) {
        int flagBits = buffer.readIntLE();

        Set<MessageFlag> flags = EnumSet.noneOf(MessageFlag.class);
        if (MessageFlag.CHECKSUM_PRESENT.isSet(flagBits)) {
            flagBits = MessageFlag.CHECKSUM_PRESENT.removeFrom(flagBits);
            flags.add(MessageFlag.CHECKSUM_PRESENT);
        }

        if (flagBits != 0) {
            throw new UnsupportedOperationException("flags=" + flagBits + " not yet supported");
        }

        int expectedPayloadSize = header.getTotalLength() - LENGTH_FIELD_LENGTH;
        if (flags.contains(MessageFlag.CHECKSUM_PRESENT)) {
            expectedPayloadSize -= CHECKSUM_LENGTH;
        }

        RawBsonDocument body = null;
        while (buffer.readerIndex() < expectedPayloadSize) {
            byte sectionKind = buffer.readByte();
            switch (sectionKind) {
                case MongoMessage.SECTION_KIND_BODY:
                    Assert.isNull(body);
                    byte[] bytes = new byte[buffer.readableBytes()];
                    buffer.readBytes(bytes);
                    body = new RawBsonDocument(bytes);
                    break;
                case MongoMessage.SECTION_KIND_DOCUMENT_SEQUENCE:
                    // seems SECTION_KIND_DOCUMENT_SEQUENCE is a legacy type, just ignore this
                default:
                    throw new IllegalArgumentException("Unexpected section kind: " + sectionKind);
            }
        }

        if (flags.contains(MessageFlag.CHECKSUM_PRESENT)) {
            int checksum = buffer.readIntLE();
            //NOTE(deyukong): dont validate checksum because dds c++ code also ignores this, I don't know why
            log.trace("Ignoring checksum {}", checksum);
        }

        Assert.notNull(body);
        return new MongoMessage(header, body, false);
    }
}
