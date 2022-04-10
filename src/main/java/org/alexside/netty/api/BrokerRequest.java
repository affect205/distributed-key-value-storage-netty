package org.alexside.netty.api;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class BrokerRequest implements Serializable {
    public final String uuid;
    public final RequestType requestType;
    public final String content;

    private BrokerRequest(RequestType requestType, String content) {
        this.uuid = UUID.randomUUID().toString();
        this.requestType = requestType;
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerRequest)) return false;
        BrokerRequest that = (BrokerRequest) o;
        return uuid.equals(that.uuid) && requestType == that.requestType && content.equals(that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, requestType, content);
    }

    @Override
    public String toString() {
        return String.format("BrokerRequest(uuid=%s, requestType=%s, content=%s)", uuid, requestType, content);
    }

    public static BrokerRequest of(RequestType requestType, String content) {
        return new BrokerRequest(requestType, content);
    }

    public static BrokerRequest get(String content) {
        return BrokerRequest.of(RequestType.GET, content);
    }

    public static BrokerRequest put(String content) {
        return BrokerRequest.of(RequestType.PUT, content);
    }

    public static BrokerRequest close() {
        return BrokerRequest.of(RequestType.CLOSE, "");
    }

    public static BrokerRequest sync() { return BrokerRequest.of(RequestType.SYNC, ""); }

    public static BrokerRequest syncAck() { return BrokerRequest.of(RequestType.SYNC_ACK, ""); }
}
