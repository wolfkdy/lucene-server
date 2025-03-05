package com.example.commands;

import com.example.protocol.MongoMessage;
import com.example.server.MessageProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.bson.RawBsonDocument;

import java.util.HashMap;

public abstract class Command {
    private final String name;
    private final String oldName;
    Command(String name) {
        this.name = name;
        this.oldName = null;
    }
    Command(String name, String oldName) {
        this.name = name;
        this.oldName = oldName;
    }
    public abstract RawBsonDocument run(ChannelHandlerContext opCtx, MongoMessage msg);
    public void register(HashMap<String, Command> m) {
        m.put(name, this);
        if (oldName != null) {
            m.put(oldName, this);
        }
    }
}
 