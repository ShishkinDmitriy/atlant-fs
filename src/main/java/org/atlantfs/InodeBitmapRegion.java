package org.atlantfs;

/**
 * Represent several blocks region with bitmap about free inodes.
 */
class InodeBitmapRegion extends BitmapRegion<Inode.Id, Inode.Range> {

    InodeBitmapRegion(AtlantFileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    Inode.Id applyOffset(int bitmapNumber, int position) {
        return Inode.Id.of(bitmapNumber * blockSize() * 8 + position + 1);
    }

    @Override
    Inode.Range applyOffset(int bitmapNumber, Bitmap.Range range) {
        return Inode.Range.of(applyOffset(bitmapNumber, range.from()), range.length());
    }

    @Override
    public Block.Id firstBlock() {
        return fileSystem.superBlock().firstBlockOfInodeBitmap();
    }

    @Override
    public int numberOfBlocks() {
        return fileSystem.superBlock().numberOfInodeBitmaps();
    }

    @Override
    public int blockSize() {
        return fileSystem.superBlock().blockSize();
    }

    @Override
    int toBitmapNumber(Inode.Id id) {
        return id.value() / (blockSize() * 8);
    }

    @Override
    int toBitmapOffset(Inode.Id id) {
        return (id.value() - 1) % (blockSize() * 8);
    }

}
