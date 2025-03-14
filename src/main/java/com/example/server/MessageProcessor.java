package com.example.server;

import com.example.commands.*;
import com.example.protocol.MongoMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.RawBsonDocument;


import java.io.IOException;
import java.util.HashMap;

public class MessageProcessor {
    private static final Logger log = LogManager.getLogger(MessageProcessor.class);

    private final HashMap<String, Command> cmdMap;

    MessageProcessor() {
        cmdMap = new HashMap<String, Command>();
        Command [] cmds = {
                new CmdIsMaster(),
                new CmdCreateVectorIndex(),
                new CmdBatchWriteHnsw(),
                new CmdVectorSearch(),
                new CmdBeginBackup(),
                new CmdEndBackup(),
                new CmdAdvanceWriteTimestamp(),
                new CmdListIndexes(),
                new CmdDropIndex(),
        };
        for (Command c : cmds) {
            c.register(cmdMap);
        }
    }

    public RawBsonDocument handleMessage(ChannelHandlerContext opCtx, MongoMessage msg) throws IOException {
        MongoMessage rsp = null;
        log.debug("get cmd name {}", msg.getCommandName());
        Command cmd = cmdMap.get(msg.getCommandName());
        if (cmd == null) {
            return Command.createErrRspWithMsg(msg.getCommandName() + " is not a valid command name");
        }
        try {
            return cmd.run(opCtx, msg);
        } catch (IOException e) {
            return Command.createErrRspWithMsg(e.toString());
        }
    }
}
