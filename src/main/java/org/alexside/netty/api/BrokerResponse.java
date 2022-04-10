package org.alexside.netty.api;

import java.io.Serializable;
import java.util.Objects;

public class BrokerResponse implements Serializable {
    public final String uuid;
    public final ResponseType responseType;
    public final String content;

    private BrokerResponse(String uuid, ResponseType responseType, String content) {
        this.uuid = uuid;
        this.responseType = responseType;
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerResponse)) return false;
        BrokerResponse response = (BrokerResponse) o;
        return uuid.equals(response.uuid) && responseType == response.responseType && content.equals(response.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, responseType, content);
    }

    @Override
    public String toString() {
        return String.format("BrokerResponse(requestId=%s, responseType=%s, content=%s)",
                uuid, responseType, content);
    }

    public static BrokerResponse of(String requestId, ResponseType responseType, String content) {
        return new BrokerResponse(requestId, responseType, content);
    }

    public static BrokerResponse putFormatError(String uuid) {
        return BrokerResponse.of(uuid, ResponseType.ERROR,
                "Invalid content format! Content must contain ':' separator with non empty key and value.");
    }
}
