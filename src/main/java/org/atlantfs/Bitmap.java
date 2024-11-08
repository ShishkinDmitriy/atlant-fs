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

    private final BitSet bitset;
    private final int length;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean dirty;

    Bitmap(BitSet bitset, int length) {
        this.bitset = bitset;
        this.length = length;
        checkInvariant();
    }

    static Bitmap read(ByteBuffer buffer) {
        var remaining = buffer.remaining();
        var bitSet = BitSet.valueOf(buffer);
        return new Bitmap(bitSet, remaining);
    }

    void write(ByteBuffer buffer) {
        var remaining = buffer.remaining();
        if (remaining < length) {
            throw new IllegalArgumentException("Buffer size [" + remaining + "] mismatch with bitmap size [" + length + "]");
        }
        if (!dirty) {
            return;
        }
        dirty = false;
        buffer.put(bitset.toByteArray());
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
        dirty = true;
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
        dirty = true;
        checkInvariant();
        return ranges;
    }

    boolean free(int position) {
        if (!bitset.get(position)) {
            return false;
        }
        bitset.set(position, false);
        dirty = true;
        checkInvariant();
        return true;
    }

    void free(Range range) {
        bitset.set(range.from, range.from + range.length, false);
        dirty = true;
        checkInvariant();
    }

    void lock() {
        this.lock.lock();
    }

    void unlock() {
        this.lock.unlock();
    }

    void checkInvariant() {
        assert bitset.length() <= length * 8 : "Bitmap length [bits=" + bitset.length() + "] should be not more then total [bits=" + length * 8 + "]";
    }

    static int byteToBits(int b) {
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
