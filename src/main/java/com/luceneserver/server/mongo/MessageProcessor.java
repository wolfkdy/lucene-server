package com.luceneserver.server.mongo;

import com.luceneserver.commands.mongo.*;
import com.luceneserver.protocol.mongo.MongoMessage;
import com.luceneserver.protocol.mongo.MsgHeader;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.RawBsonDocument;


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MessageProcessor {
    private static final Logger log = LogManager.getLogger(MessageProcessor.class);

    private final HashMap<String, Command> cmdMap;

    public static class Config {
        public int minPoolSize = 10;
        public int maxPoolSize = 1000;
        public int threadIdleMillis = 30000;
    }
    ThreadPoolExecutor executor;

    MessageProcessor(Config cfg) {
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

        executor = new ThreadPoolExecutor(
                cfg.minPoolSize,
                cfg.maxPoolSize,
                cfg.threadIdleMillis,
                TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<Runnable>()
        );
    }

    void shutdown() {
        //Initiates an orderly shutdown in which previously submitted tasks are executed,
        //but no new tasks will be accepted.
        executor.shutdown();

        try {
            //Waits for a maximum of 3 seconds for all tasks to complete execution
            //after shutdown request.
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                //Force shutdown if not all tasks could complete in the given timeout
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            //If the current thread is interrupted while waiting
            executor.shutdownNow();
        }
    }

    public void process(ChannelHandlerContext opCtx, MongoMessage msg, MsgHeader rspMsgHeader) {
        executor.execute(() -> {
            RawBsonDocument rsp = handleMessage(opCtx, msg);
            MongoMessage rspMsg = new MongoMessage(rspMsgHeader, rsp, msg.isLegacyFormat());
            opCtx.channel().writeAndFlush(rspMsg);
        });
    }

    public RawBsonDocument handleMessage(ChannelHandlerContext opCtx, MongoMessage msg) {
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
