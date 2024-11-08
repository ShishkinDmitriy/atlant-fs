package org.atlantfs;

/**
 * Represent several blocks region with bitmap about free data blocks.
 */
class DataBitmapRegion extends AbstractBitmapRegion<Block.Id, Block.Range> {

    DataBitmapRegion(AtlantFileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    Block.Id applyOffset(int bitmapNumber, int position) {
        return Block.Id.of(bitmapNumber * blockSize() * 8 + position);
    }

    @Override
    Block.Range applyOffset(int bitmapNumber, Bitmap.Range range) {
        return Block.Range.of(applyOffset(bitmapNumber, range.from()), range.length());
    }

    @Override
    public Block.Id firstBlock() {
        return fileSystem.getSuperBlock().getBlockBitmapFirstBlock();
    }

    @Override
    public int numberOfBlocks() {
        return fileSystem.getSuperBlock().getBlockBitmapNumberOfBlocks();
    }

    @Override
    public int blockSize() {
        return fileSystem.getSuperBlock().getBlockSize();
    }


}
