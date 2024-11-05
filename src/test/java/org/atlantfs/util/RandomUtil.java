package org.atlantfs.util;

import java.util.Random;

public class RandomUtil {

    private static final Random RANDOM = new Random();

    public static String randomString(int length) {
        return RANDOM.ints('a', 'z')
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public static int randomInt() {
        return RANDOM.nextInt();
    }

}
