package org.alexside.netty.worker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.alexside.netty.api.BrokerRequest;
import org.alexside.netty.api.BrokerResponse;
import org.alexside.netty.api.ResponseType;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static java.lang.String.format;

/**
 * Handles both client-side and server-side handler depending on which
 * constructor was called.
 */
public class WorkerServerHandler extends ChannelInboundHandlerAdapter {

    public static final Logger log = Logger.getLogger(WorkerServerHandler.class.getName());

    private final String brokerHost;
    private final int brokerPort;

    private static final Map<String, String> keyValues = new ConcurrentHashMap<>();

    public WorkerServerHandler(String brokerHost, int brokerPort) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        log.info("Channel is active. Trying to connect to broker..");
        ctx.writeAndFlush(BrokerRequest.sync());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        BrokerRequest request = (BrokerRequest)msg;
        log.info(format("Received: %s", request));

        switch (request.requestType) {
            case GET:
                ctx.write(BrokerResponse.of(request.uuid, ResponseType.SUCCESS, keyValues.get(request.content)));
                break;

            case PUT:
                if (!request.content.contains(":")) {
                    ctx.write(BrokerResponse.putFormatError(request.uuid));
                }

                String[] keyValue = request.content.split(":");
                if (keyValue.length < 2) {
                    ctx.write(BrokerResponse.putFormatError(request.uuid));
                }

                keyValues.put(keyValue[0], keyValue[1]);
                ctx.write(BrokerResponse.of(request.uuid, ResponseType.SUCCESS, request.content));
                break;

            case CLOSE:
                log.info("Broker is shutdown..");
                ctx.close();
                break;

            case SYNC:
                SocketAddress brokerAddress = ctx.channel().remoteAddress();
                log.info(format("Synchronized with broker %s..", brokerAddress));
                break;
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
