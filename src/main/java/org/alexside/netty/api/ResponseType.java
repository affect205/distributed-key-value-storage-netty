package org.alexside.netty.api;

import static java.lang.String.format;

public enum ResponseType {
    SUCCESS, ERROR;

    public static ResponseType parse(int ordinal) {
        if (ordinal >= 0 && ordinal < values().length) {
            return values()[ordinal];
        } else {
            throw new IllegalArgumentException(format("ResponseType with ordinal %s is not existed.", ordinal));
        }
    }
}
