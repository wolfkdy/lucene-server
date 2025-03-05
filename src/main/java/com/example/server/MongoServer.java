package com.example.server;

import com.example.protocol.MongoWireProtocolEncoder;
import com.example.utils.MongoThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import com.example.protocol.MongoExceptionHandler;
import com.example.protocol.MongoWireProtocolHandler;

import org.slf4j.impl.SimpleLogger;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class MongoServer {
    private static final Logger log = LoggerFactory.getLogger(MongoServer.class);

    private static final int DEFAULT_NETTY_EVENT_LOOP_THREADS = 0;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelGroup channelGroup;

    // something represents the server's binding listen socket
    private Channel serverChannel;

    public void bind(String hostname, int port) {
        bind(new InetSocketAddress(hostname, port));
    }

    public void bind(SocketAddress socketAddress) {
        bind(socketAddress, DEFAULT_NETTY_EVENT_LOOP_THREADS, DEFAULT_NETTY_EVENT_LOOP_THREADS);
    }

    public void bind(SocketAddress socketAddress, int numberOfBossThreads, int numberOfWorkerThreads) {
        bossGroup = new NioEventLoopGroup(numberOfBossThreads, new MongoThreadFactory("mongo-server-boss"));
        workerGroup = new NioEventLoopGroup(numberOfWorkerThreads, new MongoThreadFactory("mongo-server-worker"));
        channelGroup = new DefaultChannelGroup("mongodb-channels", workerGroup.next());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .localAddress(socketAddress)
                    .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new MongoWireProtocolEncoder());
                            ch.pipeline().addLast(new MongoWireProtocolHandler());
                            ch.pipeline().addLast(new MongoServerHandler(channelGroup));
                            ch.pipeline().addLast(new MongoExceptionHandler());
                        }
                    });

            serverChannel = bootstrap.bind().syncUninterruptibly().channel();

            log.info("started {}", this);
        } catch (RuntimeException e) {
            shutdown();
            throw e;
        }
    }

    /**
     * Stop accepting new clients. Wait until all resources (such as client
     * connection) are closed and then shutdown. This method blocks until all
     * clients are finished.
     */
    public synchronized void shutdown() {
        stopListening();
        closeClients();
        // Shut down all event loops to terminate all threads.
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }

        if (bossGroup != null) {
            bossGroup.terminationFuture().syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.terminationFuture().syncUninterruptibly();
        }

        log.info("completed shutdown of {}", this);
    }

    /**
     * Closes the server socket. No new clients are accepted afterwards.
     */
    public void stopListening() {
        if (serverChannel != null) {
            log.info("closing server channel");
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
    }

    /**
     * TODO(deyukong): currently waitUntilShutdown only waits for server channel close.
     * need to wait for all threads terminated.
     */
    public void waitUntilShutdown() {
        if (serverChannel != null) {
            serverChannel.closeFuture().syncUninterruptibly();
            log.info("wait for server channel closed succ");
        }
    }
    private void closeClients() {
        if (channelGroup != null) {
            int numClients = channelGroup.size();
            if (numClients > 0) {
                log.warn("Closing {} clients", numClients);
            }
            channelGroup.close().syncUninterruptibly();
            channelGroup = null;
        }
    }

    public static void main(String[] args) {
        System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
        MongoServer server = new MongoServer();
        server.bind("127.0.0.1", 27017);
        Signal.handle(new Signal("INT"), new SignalHandler() {
            @Override
            public void handle(Signal sig) {
                server.shutdown();
            }
        });
        server.waitUntilShutdown();
    }
}
