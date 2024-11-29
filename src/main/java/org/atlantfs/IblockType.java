package org.atlantfs;

import java.nio.ByteBuffer;

enum IblockType {

    FILE_INLINE_DATA(1, FileType.REGULAR_FILE, (fileSystem, buffer, size, _) -> DataIblock.read(fileSystem, buffer, size)),

    FILE_BLOCK_MAPPING(2, FileType.REGULAR_FILE, (fileSystem, buffer, size, _) -> FileBlockMapping.read(fileSystem, buffer, size)),

    FILE_EXTENT_TREE(3, FileType.REGULAR_FILE, (_, _, _, _) -> null), // Unsupported yet

    DIR_INLINE_LIST(4, FileType.DIRECTORY, (_, buffer, _, _) -> DirListIblock.read(buffer)),

    DIR_BLOCK_MAPPING(5, FileType.DIRECTORY, (fileSystem, buffer, _, _) -> DirBlockMapping.read(fileSystem, buffer)),

    DIR_TREE(6, FileType.DIRECTORY, (_, _, _, _) -> null); // Unsupported yet

    static final int LENGTH = 1;

    final byte value;
    final FileType fileType;
    final Factory factory;

    IblockType(int value, FileType fileType, Factory factory) {
        this.value = (byte) value;
        this.fileType = fileType;
        this.factory = factory;
    }

    static IblockType read(ByteBuffer buffer) {
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

    Iblock create(AtlantFileSystem fileSystem, ByteBuffer buffer, long size, int blocksCount) {
        return factory.create(fileSystem, buffer, size, blocksCount);
    }

    void write(ByteBuffer buffer) {
        buffer.put(value);
    }

    interface Factory {

        Iblock create(AtlantFileSystem fileSystem, ByteBuffer buffer, long size, int blocksCount);

    }

}
