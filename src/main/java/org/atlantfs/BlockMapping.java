package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * Cache impact (org.atlantfs.func.CreateFileTest#soapOpera(org.junit.jupiter.api.TestInfo):
 * <pre>
 * Cache.NoOp -> Statistics: [readCalls=77774, readBytes=9955072, writeCalls=5202, writeBytes=305910]
 * Cache      -> Statistics: [readCalls=300, readBytes=38400, writeCalls=5190, writeBytes=305268]
 * </pre>
 */
class BlockMapping implements DirectoryOperations, FileOperations, IBlock {

    private static final Logger log = Logger.getLogger(BlockMapping.class.getName());

    private final List<Block.Id> addresses;
    private final Inode inode;
    private final Cache<Coordinates, Block.Id> indirectBlocksCache = new Cache<>();
    private final Cache<Block.Id, DirEntryList> dirEntryListsCache = new Cache<>();

    BlockMapping(Inode inode, List<Block.Id> addresses) {
        this.inode = inode;
        this.addresses = addresses;
    }

    static BlockMapping read(Inode inode, ByteBuffer buffer) {
        var numberOfAddresses = numberOfAddresses(inode.blockSize(), inode.inodeSize(), inode.blocksCount());
        var position = buffer.position();
        assert buffer.remaining() >= numberOfAddresses * Block.Id.LENGTH : "Buffer should have at least [" + numberOfAddresses + "] Block.Id values, but has only [" + buffer.remaining() + "] bytes";
        var blocks = new ArrayList<Block.Id>();
        for (var i = 0; i < numberOfAddresses; i++) {
            blocks.add(Block.Id.read(buffer));
        }
        buffer.position(position + Inode.iBlockLength(inode.getFileSystem().inodeSize()));
        return new BlockMapping(inode, blocks);
    }

    static BlockMapping init(Inode inode, Data data) throws BitmapRegionOutOfMemoryException {
        var blocks = new ArrayList<Block.Id>();
        if (data.hasData()) {
            var reserved = inode.reserveBlock();
            inode.writeBlock(reserved, data::write);
            blocks.add(reserved);
        }
        return new BlockMapping(inode, blocks);
    }

    static BlockMapping init(Inode inode, DirEntryList dirEntryList) throws BitmapRegionOutOfMemoryException {
        var blocks = new ArrayList<Block.Id>();
        dirEntryList.resize(inode.blockSize());
        var reserved = inode.reserveBlock();
        inode.writeBlock(reserved, dirEntryList::write);
        blocks.add(reserved);
        return new BlockMapping(inode, blocks);
    }

    @Override
    public void write(ByteBuffer buffer) {
        var iBlockLength = Inode.iBlockLength(inode.getFileSystem().inodeSize());
        assert buffer.remaining() >= iBlockLength;
        var initial = buffer.position();
        addresses.forEach(id -> id.write(buffer));
        buffer.position(initial + iBlockLength);
    }

    @Override
    public Iterator<DirEntry> iterator() {
        inode.ensureDirectory();
        return new Iterator<>() {

            int currentList = 0;
            Iterator<DirEntry> currentIterator = readDirEntryList(resolve(currentList)).iterator();

            @Override
            public boolean hasNext() {
                var currentHasNext = currentIterator.hasNext();
                if (currentHasNext) {
                    return true;
                }
                if (currentList + 1 < inode.blocksCount()) {
                    currentList++;
                    currentIterator = readDirEntryList(resolve(currentList)).iterator();
                    return currentIterator.hasNext();
                }
                return false;
            }

            @Override
            public DirEntry next() {
                return currentIterator.next();
            }

        };
    }

    @Override
    public DirEntry add(Inode.Id inodeId, FileType fileType, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        inode.ensureDirectory();
        for (int i = 0; i < blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                var add = entryList.add(inodeId, fileType, name);
                writeDirEntryList(blockId, entryList);
                return add;
            } catch (DirectoryOutOfMemoryException _) {
                // continue
            }
        }
        var reserved = allocate();
        var dirEntryList = new DirEntryList(blockSize());
        var add = dirEntryList.add(inodeId, fileType, name);
        writeDirEntryList(reserved, dirEntryList);
        return add;
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        inode.ensureDirectory();
        for (int i = 0; i < inode.blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                return entryList.get(name);
            } catch (NoSuchFileException _) {
                // continue
            }
        }
        throw new NoSuchFileException("File [" + name + "] was not found");
    }

    @Override
    public void rename(String name, String newName) throws NoSuchFileException, DirectoryOutOfMemoryException {
        inode.ensureDirectory();
        for (int i = 0; i < inode.blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                entryList.rename(name, newName);
                writeDirEntryList(blockId, entryList);
                return;
            } catch (NoSuchFileException _) {
                // continue
            }
        }
        throw new NoSuchFileException("File [" + name + "] was not found");
    }

    @Override
    public void delete(String name) throws NoSuchFileException {
        inode.ensureDirectory();
        for (int i = 0; i < inode.blocksCount(); i++) {
            try {
                var blockId = resolve(i);
                var entryList = readDirEntryList(blockId);
                entryList.delete(name);
                writeDirEntryList(blockId, entryList);
                return;
            } catch (NoSuchFileException _) {
                // continue
            }
        }
        throw new NoSuchFileException("File [" + name + "] was not found");
    }

    @Override
    public void delete() throws DirectoryNotEmptyException {
        var indirectIds = new ArrayList<Block.Id>();
        var dataIds = new ArrayList<Block.Id>();
        var blockSize = blockSize();
        var inodeSize = inodeSize();
        var numberOfDirectBlocks = numberOfDirectBlocks(inodeSize);
        for (int i = 0; i < Math.min(numberOfDirectBlocks, addresses.size()); i++) {
            dataIds.add(addresses.get(i));
        }
        var lastExistingPosition = inode.getSize() - 1;
        var lastExistingBlockNumber = (int) lastExistingPosition / blockSize;
        var offsets = addressOffsets(blockSize, inodeSize, lastExistingBlockNumber);
        for (int level = 1; level <= offsets.size(); level++) { // Over all existing indirect levels
            var root = addresses.get(numberOfDirectBlocks + level - 1);
            indirectIds.add(root);
            var offsetsOrFull = getOffsetsOrFull(offsets, level, blockSize);
            Queue<Block.Id> input = new LinkedList<>();
            Queue<Block.Id> output = new LinkedList<>();
            input.add(root);
            for (int depth = 0; depth < level; depth++) {
                while (!input.isEmpty()) {
                    var id = input.poll();
                    var count1 = input.isEmpty() ? offsetsOrFull.get(depth) + 1 : numberOfIdsPerBlock(blockSize); // If the last id then read not whole block
                    var buffer = inode.getFileSystem().readBlock(id);
                    for (int i = 0; i < count1; i++) {
                        var read = Block.Id.read(buffer);
                        output.offer(read);
                    }
                }
                if (depth == level - 1) {
                    dataIds.addAll(output);
                } else {
                    indirectIds.addAll(output);
                }
                // Now output become input and empty input become output
                var i = input;
                input = output;
                output = i;
            }
        }
        if (inode.isDirectory()) {
            for (var blockId : dataIds) {
                var entryList = readDirEntryList(blockId);
                if (!entryList.isEmpty()) {
                    throw new DirectoryNotEmptyException("Directory is not empty");
                }
            }
        }
        var ids = new ArrayList<Block.Id>();
        ids.addAll(indirectIds);
        ids.addAll(dataIds);
//        assert blocksCount() == dataIds.size();
        inode.getFileSystem().freeBlocks(ids);
    }

    /**
     * Represent occupied indexes on different levels.
     * <p>
     * Given offsets [0, 12, 15] and block size of 64 (16 ids per block)
     * should return:
     * <pre>
     * 0 -> []
     * 1 -> [15]
     * 2 -> [15, 15]
     * 3 -> [0, 12, 15]
     * </pre>
     *
     * @param offsets   list of numbers representing an offset on depth of tree
     * @param level     the level
     * @param blockSize the number of bytes per block
     * @return offsets list
     */
    private static List<Integer> getOffsetsOrFull(List<Integer> offsets, int level, int blockSize) {
        assert level > 0;
        assert level <= offsets.size();
        if (level == offsets.size()) {
            return offsets;
        }
        return IntStream.range(0, level)
                .mapToObj(_ -> numberOfIdsPerBlock(blockSize) - 1)
                .toList();
    }

    @Override
    public int write(long position, ByteBuffer buffer) throws BitmapRegionOutOfMemoryException, DirectoryOutOfMemoryException {
        var blockSize = blockSize();
        var lastExistingBlockNumber = -1;
        Block.Id lastExistingBlockId = null;
        if (inode.getSize() > 0) {
            var lastExistingPosition = inode.getSize() - 1;
            lastExistingBlockNumber = (int) lastExistingPosition / blockSize;
            lastExistingBlockId = resolve(lastExistingBlockNumber);
            var firstRequiredBlockNumber = (int) ((position + buffer.position()) / blockSize);
            if (lastExistingBlockNumber == firstRequiredBlockNumber) {
                // Need to fill with zeros all space from last written byte up to required start position
                var offset = (int) ((lastExistingPosition + 1) % blockSize);
                var requiredOffset = (int) ((position + buffer.position()) % blockSize);
                var length = requiredOffset - offset;
                if (length > 0) {
                    writeEmptyData(lastExistingBlockId, offset, length);
                }
            }
            if (lastExistingBlockNumber < firstRequiredBlockNumber) {
                // There is a gap on end, then possibly N empty blocks and new block with gap on start.
                // Fill lastExistingBlockNumber by zeros up to the end
                {
                    var offset = (int) (lastExistingPosition % blockSize) + 1;
                    var length = blockSize - offset;
                    if (length > 0) {
                        writeEmptyData(lastExistingBlockId, offset, length);
                    }
                }
                for (int i = 0; i < firstRequiredBlockNumber - lastExistingBlockNumber - 1; i++) {
                    lastExistingBlockId = allocate();
                    lastExistingBlockNumber++;
                    writeEmptyData(lastExistingBlockId, 0, blockSize);
                }
                {
                    lastExistingBlockId = allocate();
                    lastExistingBlockNumber++;
                    var offset = (int) position % blockSize;
                    if (offset > 0) {
                        writeEmptyData(lastExistingBlockId, 0, offset);
                    }
                }
            }
        }
        var totalWritten = 0;
        while (buffer.hasRemaining()) {
            var positionPlus = position + buffer.position();
            var blockNumber = (int) (positionPlus / blockSize);
            var offset = (int) (positionPlus % blockSize);
            var length = Math.min(blockSize - offset, buffer.remaining());
            if (blockNumber > lastExistingBlockNumber) {
                lastExistingBlockId = allocate();
                lastExistingBlockNumber++;
            }
            var written = writeData(lastExistingBlockId, offset, buffer.slice(buffer.position(), length));
            totalWritten += written;
            buffer.position(buffer.position() + written);
            if (written < length) {
                break;
            }
        }
        assert !buffer.hasRemaining();
        return totalWritten;
    }

    @Override
    public int read(long position, ByteBuffer buffer) {
        var blockSize = blockSize();
        var totalRead = 0;
        while (buffer.hasRemaining()) {
            var positionPlus = position + buffer.position();
            var blockNumber = (int) (positionPlus / blockSize);
            if (positionPlus >= inode.getSize()) {
                return totalRead;
            }
            var offset = (int) (positionPlus % blockSize);
            var length = Math.min(blockSize - offset, buffer.remaining());
            var blockId = resolve(blockNumber);
            var slice = inode.getFileSystem().readBlock(blockId).slice(offset, length);
            buffer.put(slice);
            totalRead += length;
        }
        return totalRead;
    }

    Block.Id resolve(int blockNumber) {
        log.finer(() -> "Resolving [blockNumber=" + blockNumber + ", inodeId=" + inode.getId() + ", addresses=" + addresses + "]...");
        var blocksCount = blocksCount();
        var inodeSize = inodeSize();
        var blockSize = blockSize();
        if (blockNumber >= blocksCount) {
            throw new IndexOutOfBoundsException("Block number [blockNumber=" + blockNumber + "] is out of bounds [blocksCount=" + blocksCount + "]");
        }
        var numberOfDirectBlocks = numberOfDirectBlocks(inodeSize);
        if (blockNumber < numberOfDirectBlocks) {
            var blockId = addresses.get(blockNumber);
            log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + blockId + "] by direct");
            return blockId;
        }
        var level = addressLevel(blockSize, inodeSize, blockNumber);
        var offsets = addressOffsets(blockSize, inodeSize, blockNumber);
        assert addresses.size() >= numberOfDirectBlocks + level;
        var blockId = addresses.get(numberOfDirectBlocks - 1 + level);
        for (var offset : offsets) {
            blockId = readBlockId(blockId, offset);
        }
        Block.Id finalBlockId = blockId;
        log.fine(() -> "Successfully resolved [blockNumber=" + blockNumber + ", blockId=" + finalBlockId + "] by [" + level + "] level indirect");
        return blockId;
    }

    Block.Id allocate() throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException {
        checkInvariant();
        var blockSize = blockSize();
        var inodeSize = inodeSize();
        var blocksCount = blocksCount();
        log.finer(() -> "Allocating new block [blockNumber=" + blocksCount + ", inodeId=" + inode.getId() + ", addresses=" + addresses + "]...");
        if (!canGrow(blockSize, inodeSize, blocksCount)) {
            throw new DirectoryOutOfMemoryException("Not enough memory to allocate new block");
        }
        var numberOfDirectBlocks = numberOfDirectBlocks(inodeSize);
        var offsets = addressOffsets(blockSize, inodeSize, blocksCount);
        log.finer(() -> "Calculated offsets [offsets=" + offsets + "]...");
        var newIndirectBlocksCount = numberOfTrailingZeros(offsets);
        var reserved = inode.reserveBlocks(newIndirectBlocksCount + 1);
        var ids = flat(reserved); // Last is data block
        for (int i = ids.size() - 2; i >= 0; i--) {
            writeBlockId(ids.get(i), 0, ids.get(i + 1));
        }
        var result = ids.getLast();
        if (offsets.size() == newIndirectBlocksCount) {
            addresses.add(ids.getFirst());
            log.fine(() -> "Successfully allocated [blockNumber=" + blocksCount + ", blockId=" + result + "] by [" + offsets.size() + "] level indirect");
            return result;
        }
        var current = addresses.get(numberOfDirectBlocks + offsets.size() - 1);
        var lastNonZeroLevel = offsets.size() - newIndirectBlocksCount - 1;
        for (int i = 0; i < lastNonZeroLevel; i++) {
            current = readBlockId(current, offsets.get(i));
        }
        writeBlockId(current, offsets.get(lastNonZeroLevel), ids.getFirst());
        checkInvariant();
        log.fine(() -> "Successfully allocated [blockNumber=" + blocksCount + ", blockId=" + result + "] by [" + offsets.size() + "] level indirect");
        return result;
    }

    private static int numberOfTrailingZeros(List<Integer> offsets) {
        var result = 0;
        var cursor = offsets.size() - 1;
        while (cursor >= 0 && offsets.get(cursor) == 0) {
            result++;
            cursor--;
        }
        return result;
    }

    List<Block.Id> flat(List<Block.Range> ranges) {
        return ranges.stream()
                .flatMap(range -> IntStream.range(0, range.length())
                        .mapToObj(i -> range.from().plus(i)))
                .toList();
    }

    Block.Id readBlockId(Block.Id blockId, int offset) {
        return readBlockId(Coordinates.of(blockId, offset));
    }

    Block.Id readBlockId(Coordinates coordinates) {
        return indirectBlocksCache.computeIfAbsent(coordinates, key -> {
            log.fine(() -> "Reading blockId from [coordinates=" + key + "]...");
            var buffer = inode.readBlock(key.blockId);
            buffer.position(key.offset * Block.Id.LENGTH);
            var blockId = Block.Id.read(buffer);
            assert !blockId.equals(Block.Id.ZERO);
            log.fine(() -> "Successfully read [blockId=" + blockId + "] from [coordinates=" + key + "]");
            return blockId;
        });
    }

    void writeBlockId(Block.Id blockId, int offset, Block.Id value) {
        log.fine(() -> "Writing blockId into [blockId=" + blockId + ", offset=" + offset + ", value=" + value + "]...");
        inode.writeBlock(blockId, offset * Block.Id.LENGTH, value::write);
        indirectBlocksCache.put(Coordinates.of(blockId, offset), value);
    }

    int writeEmptyData(Block.Id blockId, int offset, int length) {
        return inode.getFileSystem().writeBlockEmpty(blockId, offset, length);
    }

    int writeData(Block.Id blockId, int offset, ByteBuffer buffer) {
        return inode.getFileSystem().writeBlockData(blockId, offset, buffer);
    }

    DirEntryList readDirEntryList(Block.Id blockId) {
        return dirEntryListsCache.computeIfAbsent(blockId, key -> {
            var buffer = inode.readBlock(key);
            return DirEntryList.read(buffer);
        });
    }

    private void writeDirEntryList(Block.Id reserved, DirEntryList dirEntryList) {
        inode.writeBlock(reserved, dirEntryList::write);
        dirEntryListsCache.put(reserved, dirEntryList);
    }

    private int blockSize() {
        return inode.blockSize();
    }

    private int inodeSize() {
        return inode.inodeSize();
    }

    private int blocksCount() {
        return inode.blocksCount();
    }

    private void checkInvariant() {
        assert addresses.size() == numberOfAddresses(blockSize(), inodeSize(), blocksCount());
    }

    static int addressLevel(int blockSize, int inodeSize, long blockNumber) {
        var numberOfDirectBlocks = numberOfDirectBlocks(inodeSize);
        if (blockNumber < numberOfDirectBlocks) {
            return 0;
        }
        blockNumber -= numberOfDirectBlocks;
        var numberOfIdsPerBlock = numberOfIdsPerBlock(blockSize);
        int result = 0;
        do {
            result++;
            blockNumber -= (int) Math.pow(numberOfIdsPerBlock, result);
        } while (blockNumber >= 0);
        return result;
    }

    static List<Integer> addressOffsets(int blockSize, int inodeSize, int blockNumber) {
        var level = addressLevel(blockSize, inodeSize, blockNumber);
        if (level == 0) {
            return new LinkedList<>();
        }
        blockNumber -= numberOfDirectBlocks(inodeSize);
        var numberOfIdsPerBlock = numberOfIdsPerBlock(blockSize);
        for (int i = 1; i < level; i++) {
            blockNumber -= (int) Math.pow(numberOfIdsPerBlock, i);
        }
        var result = new LinkedList<Integer>();
        for (int i = 0; i < level; i++) {
            result.addFirst(blockNumber % numberOfIdsPerBlock);
            blockNumber /= numberOfIdsPerBlock;
        }
        return result;
    }

    static boolean canGrow(int blockSize, int inodeSize, int blocksCount) {
        var futureNumberOfAddresses = numberOfAddresses(blockSize, inodeSize, blocksCount + 1);
        var maxNumberOfAddresses = numberOfDirectBlocks(inodeSize) + numberOfIndirectLevels();
        return futureNumberOfAddresses <= maxNumberOfAddresses;
    }

    static int numberOfAddresses(int blockSize, int inodeSize, int blocksCount) {
        return Math.min(blocksCount, numberOfDirectBlocks(inodeSize)) + addressLevel(blockSize, inodeSize, blocksCount - 1);
    }

    static int numberOfDirectBlocks(int inodeSize) {
        return Inode.iBlockLength(inodeSize) / Block.Id.LENGTH - numberOfIndirectLevels();
    }

    static int numberOfIdsPerBlock(int blockSize) {
        return blockSize / Block.Id.LENGTH;
    }

    static int numberOfIndirectLevels() {
        return 3;
    }

    List<Block.Id> getAddresses() {
        return Collections.unmodifiableList(addresses);
    }

    record Coordinates(Block.Id blockId, int offset) {

        static Coordinates of(Block.Id blockId, int offset) {
            return new Coordinates(blockId, offset);
        }

        @Override
        public String toString() {
            return "Coordinates{" +
                    "blockId=" + blockId +
                    ", offset=" + offset +
                    '}';
        }
    }

}
