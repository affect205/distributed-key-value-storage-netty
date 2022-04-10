package org.alexside.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.alexside.netty.api.BrokerRequest;
import org.alexside.netty.config.BrokerConfig;

import java.util.ArrayDeque;
import java.util.Deque;

public final class BrokerClient {

    public static void main(String[] args) throws Exception {
        Deque<BrokerRequest> requests = generateRequests();
        BrokerConfig config = BrokerConfig.ofClient("127.0.0.1", 8007, false, requests);
        runBrokerClient(config);
    }

    public static void runBrokerClient(BrokerConfig config) throws Exception {
        final SslContext sslCtx;
        if (config.ssl) {
            sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc(), config.brokerHost,  config.brokerPort));
                            }
                            p.addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new BrokerClientHandler(config.requests));
                        }
                    });

            // Start the connection attempt.
            b.connect(config.brokerHost,  config.brokerPort).sync().channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    public static Deque<BrokerRequest> generateRequests() {
        Deque<BrokerRequest> queue = new ArrayDeque<>();
        queue.add(BrokerRequest.get("key_1"));
        queue.add(BrokerRequest.put("key_1:value_1"));
        queue.add(BrokerRequest.get("key_1"));
//        queue.add(BrokerRequest.put("key_2:value_2"));
//        queue.add(BrokerRequest.put("key_1:value_11"));
        queue.add(BrokerRequest.close());
        return queue;
    }
}
