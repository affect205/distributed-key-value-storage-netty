package org.alexside.netty.api;

import static java.lang.String.format;

public enum RequestType {
    GET, PUT, CLOSE, SYNC, SYNC_ACK;

    public static RequestType parse(int ordinal) {
        if (ordinal >= 0 && ordinal < values().length) {
            return values()[ordinal];
        } else {
            throw new IllegalArgumentException(format("RequestType with ordinal %s is not existed.", ordinal));
        }
    }

    public static boolean isClose(int ordinal) {
        try {
            return parse(ordinal) == CLOSE;
        } catch (Exception e) {
            return false;
        }
    }
}
