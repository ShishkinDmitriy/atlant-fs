package org.atlantfs;

import java.nio.ByteBuffer;

enum FileType {

    UNKNOWN((byte) 0),
    REGULAR_FILE((byte) 1),
    DIRECTORY((byte) 2);

    static final int LENGTH = 1;

    private final byte value;

    FileType(byte value) {
        this.value = value;
    }

    static FileType read(ByteBuffer buffer) {
        var value = buffer.get();
        return switch (value) {
            case 0 -> UNKNOWN;
            case 1 -> REGULAR_FILE;
            case 2 -> DIRECTORY;
            default -> throw new IllegalArgumentException("Unknown file type [" + value + "]");
        };
    }

    void write(ByteBuffer buffer) {
        buffer.put(value);
    }

}
