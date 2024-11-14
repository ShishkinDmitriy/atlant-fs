package org.atlantfs;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class AtlantConfig {

    public static final String BLOCK_SIZE = "block-size";
    public static final String INODE_SIZE = "inode-size";
    public static final String NUMBER_OF_BLOCK_BITMAPS = "number-of-block-bitmaps";
    public static final String NUMBER_OF_INODE_BITMAPS = "number-of-inode-bitmaps";
    public static final String NUMBER_OF_INODE_TABLES = "number-of-inode-tables";
    public static final int DEFAULT_BLOCK_SIZE = 512;
    public static final int DEFAULT_INODE_SIZE = 64;
    public static final int DEFAULT_NUMBER_OF_BLOCK_BITMAPS = 1;
    public static final int DEFAULT_NUMBER_OF_INODE_BITMAPS = 1;
    public static final int DEFAULT_NUMBER_OF_INODE_TABLES = 1;

    private int blockSize = DEFAULT_BLOCK_SIZE;
    private int inodeSize = DEFAULT_INODE_SIZE;
    private int numberOfBlockBitmaps = DEFAULT_NUMBER_OF_BLOCK_BITMAPS;
    private int numberOfInodeBitmaps = DEFAULT_NUMBER_OF_INODE_BITMAPS;
    private int numberOfInodeTables = DEFAULT_NUMBER_OF_INODE_TABLES;

    private AtlantConfig() {
    }

    public static AtlantConfig defaults() {
        return new AtlantConfig();
    }

    public static AtlantConfig fromMap(Map<String, ?> map) {
        var config = new AtlantConfig();
        setIfPresent(map.get(BLOCK_SIZE), config::blockSize);
        setIfPresent(map.get(INODE_SIZE), config::inodeSize);
        setIfPresent(map.get(NUMBER_OF_BLOCK_BITMAPS), config::numberOfBlockBitmaps);
        setIfPresent(map.get(NUMBER_OF_INODE_BITMAPS), config::numberOfInodeBitmaps);
        setIfPresent(map.get(NUMBER_OF_INODE_TABLES), config::numberOfInodeTables);
        return config;
    }

    private static void setIfPresent(Object value, Consumer<Integer> consumer) {
        Optional.ofNullable(value)
                .filter(Integer.class::isInstance)
                .map(Integer.class::cast)
                .ifPresent(consumer);
    }

    public Map<String, ?> asMap() {
        return Map.of(
                BLOCK_SIZE, blockSize,
                INODE_SIZE, inodeSize,
                NUMBER_OF_BLOCK_BITMAPS, numberOfBlockBitmaps,
                NUMBER_OF_INODE_BITMAPS, numberOfInodeBitmaps,
                NUMBER_OF_INODE_TABLES, numberOfInodeTables
        );
    }

    public int blockSize() {
        return blockSize;
    }

    public int inodeSize() {
        return inodeSize;
    }

    public int numberOfBlockBitmaps() {
        return numberOfBlockBitmaps;
    }

    public int numberOfInodeBitmaps() {
        return numberOfInodeBitmaps;
    }

    public int numberOfInodeTables() {
        return numberOfInodeTables;
    }

    public AtlantConfig blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public AtlantConfig underlyingBlockSize() {
        try {
            this.blockSize = Math.toIntExact(FileSystems.getDefault()
                    .getFileStores()
                    .iterator()
                    .next()
                    .getBlockSize());
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public AtlantConfig inodeSize(int inodeSize) {
        this.inodeSize = inodeSize;
        return this;
    }

    public AtlantConfig numberOfBlockBitmaps(int numberOfBlockBitmaps) {
        this.numberOfBlockBitmaps = numberOfBlockBitmaps;
        return this;
    }

    public AtlantConfig numberOfInodeBitmaps(int numberOfInodeBitmaps) {
        this.numberOfInodeBitmaps = numberOfInodeBitmaps;
        return this;
    }

    public AtlantConfig numberOfInodeTables(int numberOfInodeTables) {
        this.numberOfInodeTables = numberOfInodeTables;
        return this;
    }

}
