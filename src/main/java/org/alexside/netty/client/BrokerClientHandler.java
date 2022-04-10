package org.alexside.netty.client;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.alexside.netty.api.BrokerRequest;
import org.alexside.netty.api.BrokerResponse;
import org.alexside.netty.api.RequestType;

import java.util.Deque;
import java.util.logging.Logger;

import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;
import static java.lang.String.format;

/**
 * Handler implementation for the object echo client.  It initiates the
 * ping-pong traffic between the object echo client and server by sending the
 * first message to the server.
 */
public class BrokerClientHandler extends ChannelInboundHandlerAdapter {

    public static final Logger log = Logger.getLogger(BrokerClientHandler.class.getName());

    private final Deque<BrokerRequest> requests;

    /**
     * Creates a client-side handler.
     */
    public BrokerClientHandler(Deque<BrokerRequest> requests) {
        this.requests = requests;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (requests.isEmpty()) {
            log.warning("List of requests is empty. Closing connection..");
            ctx.close(); return;
        }

        BrokerRequest request = requests.poll();
        log.info(format("Send the first message %s", request));

        if (request.requestType == RequestType.CLOSE) {
            log.warning("Closing connection..");
            ctx.close();
        } else {
            ChannelFuture future = ctx.writeAndFlush(request);
            future.addListener(FIRE_EXCEPTION_ON_FAILURE);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        BrokerResponse response = (BrokerResponse)msg;

        log.info(format("Received: %s", response));

        if (!requests.isEmpty()) {
            BrokerRequest nextRequest = requests.poll();

            if (nextRequest.requestType == RequestType.CLOSE) {
                log.warning("Closing connection..");
                ctx.close();
            } else {
                log.info(format("Send the next message %s", nextRequest));
                ctx.write(nextRequest);
            }
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
