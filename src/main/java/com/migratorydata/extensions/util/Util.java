package com.migratorydata.extensions.util;

import com.migratorydata.extensions.user.Key;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class Util {

    static final long MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    static final long NANOSECONDS_PER_MICROSECOND = TimeUnit.MICROSECONDS.toNanos(1);

    public static String getTopicFromSubject(String subject) {
        int index = subject.indexOf("/", 1);
        if (index == -1) {
            return subject.substring(1);
        }
        return subject.substring(1, index);
    }

    // topic:randomId:access
    public static String[] getKeyElements(String token) {
        String[] elements = token.split(":");

        if (elements.length == 3) {
            return elements;
        }

        return null;
    }

    public static Key.KeyType getKeyType(String key) {
        String[] keyElements = getKeyElements(key);
        if (keyElements.length < 3) {
            throw new RuntimeException("invalid key");
        }
        if (keyElements[2].equals("s.p")) {
            return Key.KeyType.PUB_SUB;
        } else if (keyElements[2].equals("s")) {
            return Key.KeyType.SUBSCRIBE;
        } else if (keyElements[2].equals("p")) {
            return Key.KeyType.PUBLISH;
        }
        throw new RuntimeException("invalid key");
    }

    /**
     * Get the number of nanoseconds past epoch of the given {@link Instant}.
     *
     * @param instant the Java instant value
     * @return the epoch nanoseconds
     */
    public static long toEpochNanos(Instant instant) {
        return TimeUnit.NANOSECONDS.convert(instant.getEpochSecond() * MICROSECONDS_PER_SECOND + instant.getNano() / NANOSECONDS_PER_MICROSECOND, TimeUnit.MICROSECONDS);
    }
}
