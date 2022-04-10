package org.alexside.netty.config;

import org.alexside.netty.api.BrokerRequest;

import java.util.ArrayDeque;
import java.util.Deque;

public class BrokerConfig {
    private static final Deque<BrokerRequest> EMPTY_DEQUE = new ArrayDeque<>();

    public final Mode mode;
    public final String brokerHost;
    public final int brokerPort;
    public final boolean ssl;
    public final boolean interactive;
    public final Deque<BrokerRequest> requests;

    private BrokerConfig(Mode mode,
                         String brokerHost,
                         int brokerPort,
                         boolean ssl,
                         boolean interactive,
                         Deque<BrokerRequest> requests) {
        this.mode = mode;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.ssl = ssl;
        this.interactive = interactive;
        this.requests = requests;
    }

    public static BrokerConfig ofBroker(int brokerPort) {
        return new BrokerConfig(Mode.BROKER, "", brokerPort, false, false, EMPTY_DEQUE);
    }

    public static BrokerConfig ofWorker(String brokerHost, int brokerPort) {
        return new BrokerConfig(Mode.WORKER, brokerHost, brokerPort, false, false, EMPTY_DEQUE);
    }

    public static BrokerConfig ofClient(String brokerHost, int brokerPort) {
        return new BrokerConfig(Mode.CLIENT, brokerHost, brokerPort,  false, false, EMPTY_DEQUE);
    }

    public static BrokerConfig ofClient(String brokerHost, int brokerPort, boolean interactive, Deque<BrokerRequest> requests) {
        return new BrokerConfig(Mode.CLIENT, brokerHost, brokerPort, false, interactive, requests);
    }

    enum Mode {
        BROKER, WORKER, CLIENT
    }
}
