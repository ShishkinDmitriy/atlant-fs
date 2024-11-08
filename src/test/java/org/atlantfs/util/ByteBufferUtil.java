package org.atlantfs.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

public class ByteBufferUtil {

    public static ByteBuffer byteBuffer(String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex.replaceAll(" ", ""));
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer;
    }

    public static List<ByteBuffer> blocks(String blocksHex) {
        return Arrays.stream(blocksHex.trim().split(" "))
                .map(v -> HexFormat.of().parseHex(v))
                .map(ByteBuffer::wrap)
                .map(b -> b.order(ByteOrder.LITTLE_ENDIAN))
                .toList();
    }

}
