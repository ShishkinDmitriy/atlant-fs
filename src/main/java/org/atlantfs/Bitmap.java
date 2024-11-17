package org.atlantfs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

class Bitmap {

    private static final Logger log = Logger.getLogger(Bitmap.class.getName());

    static final int NOT_FOUND = -1;

    private final Block.Id blockId;
    private final BitSet bitset;
    private final int length;
    private final ReentrantLock lock = new ReentrantLock();
    private int dirtyMin = Integer.MAX_VALUE;
    private int dirtyMax = Integer.MIN_VALUE;
    private boolean dirty;

    Bitmap(Block.Id blockId, BitSet bitset, int length) {
        this.blockId = blockId;
        this.bitset = bitset;
        this.length = length;
        checkInvariant();
    }

    static Bitmap read(ByteBuffer buffer, Block.Id blockId) {
        var bitSet = BitSet.valueOf(buffer);
        return new Bitmap(blockId, bitSet, buffer.capacity());
    }

    void write(ByteBuffer buffer) {
        var remaining = buffer.remaining();
        if (remaining < length) {
            throw new IllegalArgumentException("Buffer size [" + remaining + "] mismatch with bitmap size [" + length + "]");
        }
        buffer.put(bitset.toByteArray());
        var written = remaining - buffer.remaining();
        for (var i = written; i <= (dirtyMax / 8); i++) { // In case of setting free it shrinks, need to write manually
            buffer.put((byte) 0);
        }
        resetDirty();
        checkInvariant();
    }

    int reserve() {
        log.fine(() -> "Reserving [1] bit...");
        var bit = bitset.nextClearBit(0);
        if (bit > length * 8 - 1) {
            log.finer(() -> "Next clear bit is out of bounds, nothing found");
            return NOT_FOUND;
        }
        log.finer(() -> "Next clear bit is [" + bit + "]");
        bitset.set(bit);
        markDirty(bit);
        checkInvariant();
        return bit;
    }

    List<Range> reserve(int size) {
        log.fine(() -> "Reserving [" + size + "] bits...");
        var ranges = new ArrayList<Range>();
        var allocatedSize = 0;
        var currentIndex = 0;
        while (allocatedSize < size && currentIndex < byteToBits(length)) {
            var clearBit = bitset.nextClearBit(currentIndex);
            log.finer(() -> "Found clear bit [" + clearBit + "]");
            if (clearBit > byteToBits(length) - 1) {
                break;
            }
            var setBit = bitset.nextSetBit(clearBit);
            if (setBit < 0) {
                log.finer(() -> "No more set bits, can allocate up to the end bit [" + byteToBits(length) + "]");
                setBit = byteToBits(length);
            } else {
                var setBitFinal = setBit;
                log.finer(() -> "Found set bit [" + setBitFinal + "]");
            }
            var rangeSize = Math.min(setBit - clearBit, size - allocatedSize);
            var range = Range.of(clearBit, rangeSize);
            ranges.add(range);
            log.fine(() -> "Found free range [" + range + "]...");
            bitset.set(clearBit, clearBit + range.length);
            allocatedSize += range.length;
            currentIndex = clearBit + range.length;
        }
        log.fine(() -> "Successfully found [ranges=" + ranges.size() + "] of total [blocks=" + ranges.stream().mapToInt(Range::length).sum() + "]...");
        ranges.forEach(this::markDirty);
        checkInvariant();
        return ranges;
    }

    boolean free(int position) {
        log.finer(() -> "Setting free [position=" + position + "] bit...");
        if (!bitset.get(position)) {
            log.fine(() -> "Bit [position=" + position + "] already unset");
            return false;
        }
        bitset.set(position, false);
        markDirty(position);
        checkInvariant();
        log.fine(() -> "Successfully set free [position=" + position + "] bit");
        return true;
    }

    void free(Range range) {
        bitset.set(range.from, range.from + range.length, false);
        markDirty(range);
        checkInvariant();
    }

    void lock() {
        log.fine(() -> "Locking bitmap [blockId=" + blockId + "]...");
        this.lock.lock();
    }

    void unlock() {
        log.fine(() -> "Unlocking bitmap [blockId=" + blockId + "]...");
        this.lock.unlock();
    }

    private void resetDirty() {
        dirtyMin = Integer.MAX_VALUE;
        dirtyMax = Integer.MIN_VALUE;
        dirty = false;
    }

    private void markDirty(int bit) {
        dirtyMin = Math.min(dirtyMin, bit);
        dirtyMax = Math.max(dirtyMax, bit);
        dirty = true;
    }

    private void markDirty(Range range) {
        dirtyMin = Math.min(dirtyMin, range.from);
        dirtyMax = Math.max(dirtyMax, range.from + range.length - 1);
        dirty = true;
    }

    private void checkInvariant() {
        assert bitset.length() <= length * 8 : "Bitmap length [bits=" + bitset.length() + "] should be not more then total [bits=" + length * 8 + "]";
    }

    private static int byteToBits(int b) {
        return b * 8;
    }

    //region getters/setters
    BitSet getBitset() {
        return bitset;
    }

    int getLength() {
        return length;
    }

    boolean isDirty() {
        return dirty;
    }
    //endregion

    record Range(int from, int length) {

        static Range of(int from, int length) {
            assert from >= 0;
            assert length > 0;
            return new Range(from, length);
        }

        @Override
        public String toString() {
            return "Bitmap.Range{" +
                    "from=" + from +
                    ", length=" + length +
                    '}';
        }

    }

}
