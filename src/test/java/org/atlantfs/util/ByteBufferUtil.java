package org.atlantfs.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;

public class ByteBufferUtil {
    public static ByteBuffer byteBuffer(String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex.replaceAll(" ", ""));
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer;
    }

}
