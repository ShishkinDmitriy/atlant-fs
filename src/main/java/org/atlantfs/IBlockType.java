package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

enum IBlockType {

    FILE_INLINE_DATA(1, FileType.REGULAR_FILE, (inode, buffer) -> Data.read(buffer, (int) inode.getSize())),

    FILE_BLOCK_MAPPING(2, FileType.REGULAR_FILE, BlockMapping::read),

    FILE_EXTENT_TREE(3, FileType.REGULAR_FILE, null), // Unsupported yet

    DIR_INLINE_LIST(4, FileType.DIRECTORY, (_, buffer) -> DirEntryList.read(buffer)),

    DIR_BLOCK_MAPPING(5, FileType.DIRECTORY, BlockMapping::read),

    DIR_TREE(6, FileType.DIRECTORY, (inode, buffer) -> DirTree.read(inode.getFileSystem(), buffer)); // Unsupported yet

    static final int LENGTH = 1;

    final byte value;
    final FileType fileType;
    final BiFunction<Inode, ByteBuffer, IBlock> reader;

    IBlockType(int value, FileType fileType, BiFunction<Inode, ByteBuffer, IBlock> reader) {
        this.value = (byte) value;
        this.fileType = fileType;
        this.reader = reader;
    }

    static IBlockType read(ByteBuffer buffer) {
        var value = buffer.get();
        return switch (value) {
            case 1 -> FILE_INLINE_DATA;
            case 2 -> FILE_BLOCK_MAPPING;
            case 3 -> FILE_EXTENT_TREE;
            case 4 -> DIR_INLINE_LIST;
            case 5 -> DIR_BLOCK_MAPPING;
            case 6 -> DIR_TREE;
            default -> throw new IllegalArgumentException("Unknown file type [" + value + "]");
        };
    }

    IBlock create(Inode inode, ByteBuffer buffer) {
        return reader.apply(inode, buffer);
    }

    void write(ByteBuffer buffer) {
        buffer.put(value);
    }

    FileType getFileType() {
        return fileType;
    }

}
