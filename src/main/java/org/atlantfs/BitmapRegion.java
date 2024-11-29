package org.atlantfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Represent several blocks with bitmap about free items {@link K}.
 *
 * @param <K> the type of item identifier
 * @param <R> the type of items range
 */
abstract class BitmapRegion<K extends Id, R extends Range<K>> implements Region {

    private static final Logger log = Logger.getLogger(BitmapRegion.class.getName());

    /**
     * File system.
     */
    protected final AtlantFileSystem fileSystem;

    /**
     * Cache of bitmaps blocks.
     */
    private final Cache<Block.Id, Bitmap> cache = new Cache<>();

    /**
     * The number of first bitmap with free space.
     * <p>
     * Bitmaps before assumed to be full.
     */
    private final AtomicInteger current = new AtomicInteger();

    BitmapRegion(AtlantFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    void init() {
        for (int i = 0; i < numberOfBlocks(); i++) {
            fileSystem.writeBlock(firstBlock().plus(i), buffer -> {
                while (buffer.hasRemaining()) {
                    buffer.putInt(0);
                }
            });
        }
    }

    /**
     * Reserve single {@link K}.
     *
     * @return single id of reserved item
     * @throws BitmapRegion.NotEnoughSpaceException if not enough space to reserve
     */
    K reserve() throws BitmapRegion.NotEnoughSpaceException {
        log.fine(() -> "Reserving single...");
        var cur = current.get();
        while (cur < numberOfBlocks()) {
            var id = reserveSingle(cur);
            if (id.isPresent()) {
                return id.get();
            }
            cur++;
        }
        throw new BitmapRegion.NotEnoughSpaceException();
    }

    /**
     * Reserve multiple of {@link K} items.
     *
     * @param size number of items to reserve
     * @return the list of ranges {@link R} with total of specified size
     * @throws BitmapRegion.NotEnoughSpaceException if not enough space to reserve
     */
    List<R> reserve(int size) throws BitmapRegion.NotEnoughSpaceException {
        log.fine(() -> "Reserving multiple of [size=" + size + "]...");
        var ranges = reserveMultiple(current.get(), size);
        assert ranges.stream().mapToInt(R::length).sum() == size;
        log.finer(() -> "Successfully reserved multiple of [size=" + size + ", ranges=" + ranges + "]");
        return ranges;
    }

    protected Optional<K> reserveSingle(int bitmapNumber) {
        var bitmap = loadBitmap(bitmapNumber);
        try {
            bitmap.lock();
            log.fine(() -> "Reserving [1] item in [bitmapNumber=" + bitmapNumber + "]...");
            var reserved = bitmap.reserve();
            if (reserved < 0) {
                log.fine(() -> "Bitmap [bitmapNumber=" + bitmapNumber + "] of [numberOfBlocks=" + numberOfBlocks() + "] is full, search on next bitmap...");
                // If current not decreased
                markAsOccupied(bitmapNumber);
                return Optional.empty();
            }
            checkInvariant();
            write(bitmapNumber, bitmap);
            return Optional.of(applyOffset(bitmapNumber, reserved));
        } finally {
            bitmap.unlock();
        }
    }

    protected List<R> reserveMultiple(int bitmapNumber, int size) throws BitmapRegion.NotEnoughSpaceException {
        List<Bitmap.Range> ranges = List.of();
        var bitmap = loadBitmap(bitmapNumber);
        try {
            bitmap.lock();
            log.fine(() -> "Reserving [" + size + "] items in [bitmapNumber=" + bitmapNumber + "]...");
            ranges = bitmap.reserve(size);
            if (ranges.isEmpty()) {
                log.fine(() -> "Bitmap [bitmapNumber=" + bitmapNumber + "] of [numberOfBlocks=" + numberOfBlocks() + "] is full, search on next bitmap...");
                // If current not decreased
                markAsOccupied(bitmapNumber);
            }
            var reservedSize = ranges.stream().mapToInt(Bitmap.Range::length).sum();
            var remainingSize = size - reservedSize;
            if (remainingSize > 0 && bitmapNumber >= numberOfBlocks() - 1) {
                throw new BitmapRegion.NotEnoughSpaceException();
            }
            var converted = new ArrayList<>(applyOffset(bitmapNumber, ranges));
            if (remainingSize > 0) {
                log.fine(() -> "Reserved [reservedSize=" + reservedSize + ", remainingSize=" + remainingSize + "], searching in next bitmap block...");
                var nextReserved = reserveMultiple(bitmapNumber + 1, remainingSize);
                converted.addAll(nextReserved);
            }
            checkInvariant();
            write(bitmapNumber, bitmap);
            return converted;
        } catch (BitmapRegion.NotEnoughSpaceException e) {
            ranges.forEach(bitmap::free);
            throw e;
        } finally {
            bitmap.unlock();
        }
    }

    /**
     * Mark the item {@link K} as free.
     * <p>
     * Thread safe, immediately flush changes to disk.
     *
     * @param id the identifier
     */
    void free(K id) {
        var bitmapNumber = toBitmapNumber(id);
        var bitmap = loadBitmap(bitmapNumber);
        try {
            bitmap.lock();
            var changed = bitmap.free(toBitmapOffset(id));
            if (!changed) {
                return;
            }
            markAsVacant(bitmapNumber);
            checkInvariant();
            write(bitmapNumber, bitmap);
        } finally {
            bitmap.unlock();
        }
    }

    /**
     * Mark a list of {@code K} as free.
     * <p>
     * Thread safe, immediately flush changes to disk.
     *
     * @param ids the list of identifiers to mark as free
     */
    void free(List<K> ids) {
        ids.stream()
                .collect(Collectors.groupingBy(
                        this::toBitmapNumber,
                        TreeMap::new,
                        Collectors.toList()))
                .forEach((bitmapNumber, localIds) -> {
                    var bitmap = loadBitmap(bitmapNumber);
                    try {
                        bitmap.lock();
                        localIds.forEach(id -> {
                            var bitmapOffset = toBitmapOffset(id);
                            bitmap.free(bitmapOffset);
                        });
                        markAsVacant(bitmapNumber);
                        checkInvariant();
                        write(bitmapNumber, bitmap);
                    } finally {
                        bitmap.unlock();
                    }
                });
    }

    /**
     * Mark the range {@link R} as free.
     * <p>
     * Thread safe, immediately flush changes to disk.
     * <p>
     * Thread safe notes.
     * Bitmaps should be cleared in order from lowest to highest to avoid deadlock situation when another thread
     * blocking in asc order and this thread in desc order.
     *
     * @param ranges the list of ranges to free
     */
    void freeRanges(List<R> ranges) {
        ranges.stream()
                .collect(Collectors.groupingBy(
                        range -> toBitmapNumber(range.from()),
                        TreeMap::new,
                        Collectors.toList()))
                .forEach((bitmapNumber, localRanges) -> {
                    var bitmap = loadBitmap(bitmapNumber);
                    try {
                        bitmap.lock();
                        localRanges.forEach(range -> bitmap.free(Bitmap.Range.of(toBitmapOffset(range.from()), range.length())));
                        markAsVacant(bitmapNumber);
                        checkInvariant();
                        write(bitmapNumber, bitmap);
                    } finally {
                        bitmap.unlock();
                    }
                });
    }

    void write(int bitmapNumber, Bitmap bitmap) {
        fileSystem.writeBlock(firstBlock().plus(bitmapNumber), bitmap::write);
    }

    private Bitmap loadBitmap(int bitmapNumber) {
        return cache.computeIfAbsent(firstBlock().plus(bitmapNumber), id -> {
            log.fine(() -> "Reading bitmap [bitmapNumber=" + bitmapNumber + ", block=" + id + "]...");
            var buffer = fileSystem.readBlock(id);
            var bitmap = Bitmap.read(buffer, id);
            log.finer(() -> "Successfully read bitmap [bitmapNumber=" + bitmapNumber + ", block=" + id + "]...");
            return bitmap;
        });
    }

    private void markAsOccupied(int bitmapNumber) {
        current.compareAndSet(bitmapNumber, bitmapNumber + 1);
    }

    private void markAsVacant(int bitmapNumber) {
        current.updateAndGet(v -> Math.min(bitmapNumber, v));
    }

    abstract int toBitmapNumber(K id);

    abstract int toBitmapOffset(K id);

    private List<R> applyOffset(int bitmapNumber, List<Bitmap.Range> ranges) {
        return ranges.stream()
                .map(range -> applyOffset(bitmapNumber, range))
                .toList();
    }

    abstract R applyOffset(int bitmapNumber, Bitmap.Range range);

    abstract K applyOffset(int bitmapNumber, int position);

    private void checkInvariant() {
        assert current.get() >= 0;
        assert current.get() < numberOfBlocks();
    }

    // -----------------------------------------------------------------------------------------------------------------

    int getCurrent() {
        return current.get();
    }

    void setCurrent(int value) {
        current.set(value);
    }

    public static class NotEnoughSpaceException extends org.atlantfs.NotEnoughSpaceException {

        public NotEnoughSpaceException() {
        }

        public NotEnoughSpaceException(String message) {
            super(message);
        }

        public NotEnoughSpaceException(String message, Throwable cause) {
            super(message, cause);
        }

        public NotEnoughSpaceException(Throwable cause) {
            super(cause);
        }

    }
}
