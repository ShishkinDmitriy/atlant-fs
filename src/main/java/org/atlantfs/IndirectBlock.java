package org.atlantfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

final class IndirectBlock<B extends Block> implements Block {

    private final Id id;
    private final List<Pointer<?>> pointers = new ArrayList<>();
    private final int depth;
    private final AtlantFileSystem fileSystem;
    private final List<IndirectBlock<B>> dirtyBlocks = new ArrayList<>();
    private final Function<Id, B> leafReader;
    private int size;
    private boolean dirty;

    IndirectBlock(Id id, AtlantFileSystem fileSystem, int depth, Function<Id, B> leafReader) {
        this.id = id;
        this.fileSystem = fileSystem;
        this.depth = depth;
        this.leafReader = leafReader;
    }

    /**
     * Create a new chain of indirect blocks.
     * <p>
     * The number of indirect blocks of the chain will be same as {@code depth}.
     * The last indirect block of the chain will point to leaf block.
     *
     * @param fileSystem the Atlant file system, can't be null
     * @param depth      the depth of the chain, can't be negative or zero
     * @param reader     the function to reader leaf node, can't be null
     * @param leaf       the leaf block to be immediately added to chain, can't be null
     * @param <B>        the type of leaf block
     * @return first indirect block in the chain
     * @throws BitmapRegionOutOfMemoryException if not enough memory to reserve blocks
     */
    static <B extends Block> IndirectBlock<B> init(AtlantFileSystem fileSystem, int depth, Function<Id, B> reader, B leaf) throws BitmapRegionOutOfMemoryException {
        //region preconditions
        if (fileSystem == null) throw new NullPointerException("fileSystem");
        if (depth < 0) throw new IllegalArgumentException("depth");
        if (reader == null) throw new NullPointerException("reader");
        if (leaf == null) throw new NullPointerException("leaf");
        //endregion
        var reserved = Range.flat(fileSystem.reserveBlocks(depth + 1));
        var current = new IndirectBlock<>(reserved.getLast(), fileSystem, 0, reader);
        current.pointers.add(Pointer.of(leaf, reader));
        current.dirty = true;
        current.size = 1;
        for (int i = 1; i <= depth; i++) {
            var reservedId = reserved.get(reserved.size() - i - 1);
            var indirectBlock = new IndirectBlock<>(reservedId, fileSystem, i, reader);
            int finalI = i;
            indirectBlock.pointers.add(Pointer.of(current, id -> IndirectBlock.read(fileSystem, id, finalI - 1, reader)));
            indirectBlock.dirtyBlocks.add(current);
            indirectBlock.dirty = true;
            indirectBlock.size = 1;
            current = indirectBlock;
        }
        return current;
    }

    static <B extends Block> IndirectBlock<B> read(AtlantFileSystem fileSystem, Id blockId, int depth, Function<Id, B> reader) {
        //region preconditions
        if (fileSystem == null) throw new NullPointerException("fileSystem");
        if (depth < 0) throw new IllegalArgumentException("depth");
        if (reader == null) throw new NullPointerException("reader");
        //endregion
        var buffer = fileSystem.readBlock(blockId);
        assert buffer.remaining() % Id.LENGTH == 0 : "Buffer should be aligned with BLock.Id size";
        IndirectBlock<B> indirectBlock = new IndirectBlock<>(blockId, fileSystem, depth, reader);
        while (buffer.hasRemaining()) {
            var value = Id.read(buffer);
            if (value.equals(Id.ZERO)) { // Use 0 for terminating symbol
                break;
            }
            if (depth == 0) { // Has pointers to leafs
                indirectBlock.pointers.add(Pointer.of(value, reader));
            } else { // Has pointers to another indirect blocks
                indirectBlock.pointers.add(Pointer.of(value, idd -> IndirectBlock.read(fileSystem, idd, depth - 1, reader)));
            }
        }
        indirectBlock.size = indirectBlock.readSize();
        return indirectBlock;
    }

    int readSize() {
        if (depth == 0) {
            return pointers.size();
        }
        var full = (pointers.size() - 1) * maxSize(blockSize(), depth - 1);
        //noinspection unchecked
        IndirectBlock<B> indirectBlock = (IndirectBlock<B>) pointers.getLast().get();
        return full + indirectBlock.size;
    }

    @Override
    public void flush() {
        if (!isDirty()) {
            return;
        }
        fileSystem.writeBlock(id, buffer -> {
            assert buffer.remaining() % Id.LENGTH == 0 : "Buffer should be aligned with BLock.Id size";
            pointers.forEach(pointer -> pointer.write(buffer));
            if (buffer.hasRemaining()) {
                Id.ZERO.write(buffer);
            }
        });
    }

    B get(int index) {
        //region preconditions
        if (index < 0) throw new IndexOutOfBoundsException();
        if (index >= size()) throw new IndexOutOfBoundsException();
        //endregion
        var idsPerBlock = idsPerBlock(blockSize());
        if (depth > 0) {
            var maxSize = maxSize(blockSize(), depth - 1);
            var offset = index / maxSize;
            //noinspection unchecked
            var pointer = (Pointer<IndirectBlock<B>>) pointers.get(offset);
            var indirectBlock = pointer.get();
            return indirectBlock.get(Integer.remainderUnsigned(index, maxSize));
        } else {
            if (index >= idsPerBlock) throw new IndexOutOfBoundsException();
            var offset = Integer.remainderUnsigned(index, idsPerBlock);
            //noinspection unchecked
            var pointer = (Pointer<B>) pointers.get(offset);
            return pointer.get();
        }
    }

    int add(B leaf) throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        //region preconditions
        if (size + 1 > maxSize(blockSize(), depth)) throw new IndirectBlockOutOfMemoryException("");
        //endregion
        var index = size;
        add(index, leaf);
        return index;
    }

    private void add(int index, B leaf) throws BitmapRegionOutOfMemoryException {
        if (depth > 0) {
            var maxSize = maxSize(blockSize(), depth - 1);
            var offset = Integer.divideUnsigned(index, maxSize);
            if (pointers.size() <= offset) {
                var newChain = init(fileSystem, depth - 1, leafReader, leaf);
                pointers.add(Pointer.of(newChain, this::indirectReader));
            } else {
                //noinspection unchecked
                var pointer = (Pointer<IndirectBlock<B>>) pointers.get(offset);
                var indirectBlock = pointer.get();
                indirectBlock.add(Integer.remainderUnsigned(index, maxSize), leaf);
            }
        } else {
            if (index >= idsPerBlock(blockSize())) throw new IndexOutOfBoundsException();
            pointers.add(Pointer.of(leaf, leafReader));
        }
        size++;
    }

    void delete() {
        fileSystem.freeBlock(id);
        if (depth == 0) {
            var ids = pointers.stream().map(Pointer::id).toList();
            fileSystem.freeBlocks(ids);
            return;
        }
        pointers.stream()
                .map(Pointer::get)
                .map(block -> (IndirectBlock<?>) block)
                .forEach(IndirectBlock::delete);
    }

    void addPointer(Pointer<?> pointer) {
        //region preconditions
        if (pointers.size() + 1 > idsPerBlock(blockSize())) throw new IllegalStateException();
        //endregion
        if (pointer.get() instanceof IndirectBlock<?> indirectBlock) {
            size += indirectBlock.size;
        } else {
            size++;
        }
        pointers.add(pointer);
    }

    List<? extends Block> dirtyBlocks() {
        if (dirty) {
            return Stream.concat(dirtyBlocks.stream(), Stream.of(this)).toList();
        } else {
            return Collections.unmodifiableList(dirtyBlocks);
        }
    }

    IndirectBlock<B> indirectReader(Id id) {
        return IndirectBlock.read(fileSystem, id, depth - 1, leafReader);
    }

    private int blockSize() {
        return fileSystem.blockSize();
    }

    int maxSize() {
        return maxSize(blockSize(), depth);
    }

    static int maxSize(int blockSize, int depth) {
        return (int) Math.pow(idsPerBlock(blockSize), depth + 1);
    }

    static private int idsPerBlock(int blockSize) {
        return blockSize / Id.LENGTH;
    }

    //region getters
    @Override
    public Id id() {
        return id;
    }

    int size() {
        return size;
    }

    int depth() {
        return depth;
    }

    List<Pointer<?>> pointers() {
        return Collections.unmodifiableList(pointers);
    }

    @Override
    public boolean isDirty() {
        return dirty || !dirtyBlocks.isEmpty();
    }
    //endregion

    @Override
    public String toString() {
        return "IndirectBlock{" +
                "id=" + id.value() +
                ", depth=" + depth +
                ", pointers=" + pointers.stream().map(Pointer::id).map(Id::value).toList() +
                ", size=" + size +
                ", dirty=" + isDirty() +
                '}';
    }

}
