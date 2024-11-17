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
        return firstBlockOfData().plus(bitmapNumber * blockSize() * 8 + position);
    }

    @Override
    Block.Range applyOffset(int bitmapNumber, Bitmap.Range range) {
        return Block.Range.of(applyOffset(bitmapNumber, range.from()), range.length());
    }

    @Override
    int toBitmapNumber(Block.Id id) {
        return id.minus(firstBlockOfData()).value() / (blockSize() * 8);
    }

    @Override
    int toBitmapOffset(Block.Id id) {
        return id.minus(firstBlockOfData()).value() % (blockSize() * 8);
    }

    @Override
    public Block.Id firstBlock() {
        return fileSystem.superBlock().firstBlockOfBlockBitmap();
    }

    @Override
    public int numberOfBlocks() {
        return fileSystem.superBlock().numberOfBlockBitmaps();
    }

    @Override
    public int blockSize() {
        return fileSystem.superBlock().blockSize();
    }

    private Block.Id firstBlockOfData() {
        return fileSystem.superBlock().firstBlockOfData();
    }

}
