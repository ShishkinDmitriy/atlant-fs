package org.atlantfs;

/**
 * Represent several blocks region with bitmap about free inodes.
 */
class InodeBitmapRegion extends AbstractBitmapRegion<Inode.Id, Inode.Range> {

    InodeBitmapRegion(AtlantFileSystem fileSystem) {
        super(fileSystem);
    }

    @Override
    Inode.Id applyOffset(int bitmapNumber, int position) {
        return Inode.Id.of(bitmapNumber * blockSize() * 8 + position);
    }

    @Override
    Inode.Range applyOffset(int bitmapNumber, Bitmap.Range range) {
        return Inode.Range.of(applyOffset(bitmapNumber, range.from()), range.length());
    }

    @Override
    public Block.Id firstBlock() {
        return fileSystem.getSuperBlock().getInodeBitmapFirstBlock();
    }

    @Override
    public int numberOfBlocks() {
        return fileSystem.getSuperBlock().getInodeBitmapNumberOfBlocks();
    }

    @Override
    public int blockSize() {
        return fileSystem.getSuperBlock().getBlockSize();
    }

}
