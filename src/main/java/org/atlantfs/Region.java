package org.atlantfs;

/**
 * Represents multiple {@link Block}s as single logical unit - Region.
 */
interface Region {

    Block.Id firstBlock();

    int numberOfBlocks();

    int blockSize();

}
