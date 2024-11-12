package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

enum IBlockType {

    INLINE_DATA(0, FileType.REGULAR_FILE, (_, buffer) -> Data.read(buffer)),

    DIRECT_BLOCKS(1, FileType.REGULAR_FILE, null), // Unsupported yet

    EXTENT_TREE(2, FileType.REGULAR_FILE, null), // Unsupported yet

    INLINE_DIR_LIST(3, FileType.DIRECTORY, (_, buffer) -> DirEntryList.read(buffer)),

    DIR_LIST(4, FileType.DIRECTORY, (_, buffer) -> DirEntryList.read(buffer)),

    DIR_TREE(5, FileType.DIRECTORY, DirTree::read); // Unsupported yet

    static final int LENGTH = 1;

    final byte value;
    final FileType fileType;
    BiFunction<AtlantFileSystem, ByteBuffer, IBlock> reader;

    <T> IBlockType(int value, FileType fileType, BiFunction<AtlantFileSystem, ByteBuffer, IBlock> reader) {
        this.value = (byte) value;
        this.fileType = fileType;
        this.reader = reader;
    }

    static IBlockType read(ByteBuffer buffer) {
        var value = buffer.get();
        return switch (value) {
            case 0 -> INLINE_DATA;
            case 1 -> DIRECT_BLOCKS;
            case 2 -> EXTENT_TREE;
            case 3 -> INLINE_DIR_LIST;
            case 4 -> DIR_LIST;
            case 5 -> DIR_TREE;
            default -> throw new IllegalArgumentException("Unknown file type [" + value + "]");
        };
    }

    IBlock create(AtlantFileSystem fileSystem, ByteBuffer buffer) {
        return reader.apply(fileSystem, buffer);
    }

    void write(ByteBuffer buffer) {
        buffer.put(value);
    }

    FileType getFileType() {
        return fileType;
    }

    boolean isRegularFile() {
        return this == INLINE_DATA
                || this == DIRECT_BLOCKS
                || this == EXTENT_TREE;
    }

    boolean isDirectory() {
        return this == INLINE_DIR_LIST
                || this == DIR_TREE;
    }

}
