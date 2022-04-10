package org.alexside.netty.worker;

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
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.alexside.netty.config.BrokerConfig;

public final class WorkerServer {

    public static void main(String[] args) throws Exception {
        BrokerConfig config = BrokerConfig.ofWorker("127.0.0.1", 8007 );
        runWorkerServer(config);
    }

    public static void runWorkerServer(BrokerConfig config) throws Exception {
        final SslContext sslCtx;
        if (config.ssl) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        EventLoopGroup group = new NioEventLoopGroup(1);
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
                                    new WorkerServerHandler(config.brokerHost,  config.brokerPort));
                        }
                    });

            // Bind and start to accept incoming connections.
            //b.bind(config.instancePort).sync().channel().closeFuture().sync();
            b.connect(config.brokerHost,  config.brokerPort).sync().channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
