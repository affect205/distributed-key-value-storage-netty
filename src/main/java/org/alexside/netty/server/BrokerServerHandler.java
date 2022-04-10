package org.alexside.netty.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.alexside.netty.api.BrokerRequest;
import org.alexside.netty.api.BrokerResponse;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

import static java.lang.String.format;

public class BrokerServerHandler extends ChannelInboundHandlerAdapter {

    public static final Logger log = Logger.getLogger(BrokerServerHandler.class.getName());

    private static final Map<SocketAddress, WorkerMetadata> workerMetadata = new ConcurrentHashMap<>();

    private static final Map<String, Channel> clientChannels = new ConcurrentHashMap<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof BrokerRequest) {
            requestProcessor(ctx, (BrokerRequest)msg);
        } else if (msg instanceof BrokerResponse) {
            responseProcessor((BrokerResponse)msg);
        } else {
            log.warning(format("Received unknown: %s", msg));
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

    private void requestProcessor(ChannelHandlerContext ctx, BrokerRequest request) {
        log.info(format("Received: %s", request));

        switch (request.requestType) {
            case GET:
                clientChannels.put(request.uuid, ctx.channel());
                Optional<WorkerMetadata> workerMetadataGet = findWorkerForKey(request.content);
                sendRequestToWorker(request, workerMetadataGet);
                break;

            case PUT:
                clientChannels.put(request.uuid, ctx.channel());
                if (request.content == null || !request.content.contains(":")) {
                    ctx.write(BrokerResponse.putFormatError(request.uuid));
                } else {
                    String[] keyValue = request.content.split(":");
                    if (keyValue.length < 2) {
                        ctx.write(BrokerResponse.putFormatError(request.uuid));
                    }
                    Optional<WorkerMetadata> workerMetadataPut = findWorkerForKey(keyValue[0]);
                    sendRequestToWorker(request, workerMetadataPut);
                }
                break;

            case CLOSE:
                log.info("Broker is shutdown..");
                ctx.close();
                break;

            case SYNC:
                Channel outboundWorkerChannel = ctx.channel();
                SocketAddress workerAddress = outboundWorkerChannel.remoteAddress();

                log.info(format("Synchronized with worker %s..", workerAddress));

                if (!workerMetadata.containsKey(workerAddress)) {
                    workerMetadata.put(workerAddress, new WorkerMetadata(outboundWorkerChannel));
                } else {
                    log.info(format("Worker %s already exists", workerAddress));
                }
                outboundWorkerChannel.writeAndFlush(BrokerRequest.syncAck());
                break;
        }
    }

    private void responseProcessor(BrokerResponse response) {
        log.info(format("Received: %s", response));
        Channel outboundClientChannel = clientChannels.get(response.uuid);
        if (outboundClientChannel != null) {
            outboundClientChannel.writeAndFlush(response);
            clientChannels.remove(response.uuid);
        }
    }

    private void sendRequestToWorker(BrokerRequest request, Optional<WorkerMetadata> workerMetadata) {
        workerMetadata.ifPresent(metadata -> {
            Channel outboundWorkerChannel = workerMetadata.get().outboundWorkerChannel;
            outboundWorkerChannel.writeAndFlush(request);
        });
    }

    private Optional<WorkerMetadata> findWorkerForKey(String key) {
        Optional<WorkerMetadata> metadata = workerMetadata.entrySet().stream()
                .filter(meta -> meta.getValue().workerKeys.contains(key))
                .findFirst()
                .map(Map.Entry::getValue);
        return metadata.isPresent() ? metadata : findWorkerWithLeastKeys();
    }

    private Optional<WorkerMetadata> findWorkerWithLeastKeys() {
        return new ArrayList<>(workerMetadata.values()).stream()
                .min((m1, m2) -> Integer.compare(m2.workerKeys.size(), m1.workerKeys.size()));
    }

    private static class WorkerMetadata {
        public final Channel outboundWorkerChannel;
        public final Set<String> workerKeys = new ConcurrentSkipListSet<>();

        private WorkerMetadata(Channel outboundWorkerChannel) {
            this.outboundWorkerChannel = outboundWorkerChannel;
        }
    }
}
