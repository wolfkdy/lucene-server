package com.example.server;

import com.example.storage.IndexCatalog;
import com.example.transport.MongoServerHandler;
import com.example.transport.MongoWireProtocolEncoder;
import com.example.utils.MongoThreadFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
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
import com.example.transport.MongoExceptionHandler;
import com.example.transport.MongoWireProtocolHandler;

import org.apache.logging.log4j.core.config.Configurator;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class MongoServer {


    public static class ServerConfig {
        public String host;
        public int port;
        public String logLevel;
        public String dataDir;
    }
    private static final Logger log = LogManager.getLogger(MongoServer.class);

    private static final int DEFAULT_NETTY_EVENT_LOOP_THREADS = 0;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelGroup channelGroup;

    // something represents the server's binding listen socket
    private Channel serverChannel;

    private MessageProcessor messageProcessor;

    private IndexCatalog indexCatalog;

    private Clock clock;
    private static MongoServer instance;

    public IndexCatalog getIndexCatalog() {
        return indexCatalog;
    }

    public MessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    public Clock getClock() {
        return clock;
    }
    private MongoServer() {
        clock = Clock.systemUTC();
    }

    public static MongoServer getInstance() {
        return instance;
    }
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
            log.info("started {} on {}", this, socketAddress.toString());
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

    public static ServerConfig loadConfig(String fileName) throws FileNotFoundException {
        Constructor c = new Constructor(ServerConfig.class, new LoaderOptions());
        Yaml yaml = new Yaml(c);
        InputStream is = new FileInputStream(new File(fileName));
        return yaml.load(is);
    }

    public static void main(String[] args) throws IOException {
        String cfgFileName = System.getenv("configFile");
        if (cfgFileName == null) {
            log.error("pass a configFile name by -DconfigFile=test.yaml on start");
            return;
        }
        ServerConfig serverConfig = loadConfig(cfgFileName);
        Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.valueOf(serverConfig.logLevel));
        log.info(System.getenv("configFile"));
        MongoServer server = new MongoServer();
        server.messageProcessor = new MessageProcessor();
        server.indexCatalog = new IndexCatalog(serverConfig.dataDir);
        MongoServer.instance = server;
        server.bind(serverConfig.host, serverConfig.port);
        Signal.handle(new Signal("INT"), new SignalHandler() {
            @Override
            public void handle(Signal sig) {
                server.shutdown();
            }
        });
        server.waitUntilShutdown();
    }
}
