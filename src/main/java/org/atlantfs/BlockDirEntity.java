package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * Represent single block with a list of {@link DirEntry}.
 */
final class BlockDirEntity extends Block {

    private static final Logger log = Logger.getLogger(BlockDirEntity.class.getName());

    private final List<DirEntry> entries;

    public BlockDirEntity(int length, List<DirEntry> entries) {
        this.length = length;
        this.entries = entries;
        checkInvariant();
    }

    public BlockDirEntity(int length) {
        List<DirEntry> entries = new ArrayList<>();
        entries.add(DirEntry.empty((short) length));
        this.length = length;
        this.entries = entries;
        checkInvariant();
    }

    static BlockDirEntity read(ByteBuffer buffer) {
        List<DirEntry> entries = new ArrayList<>();
        var length = buffer.remaining();
        while (buffer.hasRemaining()) {
            try {
                entries.add(DirEntry.read(buffer));
            } catch (Exception e) {
                var position = buffer.position();
                var remaining = (short) buffer.remaining();
                log.log(Level.SEVERE, "Failed to read Dir entry on position [" + position + "]", e);
                if (entries.isEmpty()) {
                    entries.add(DirEntry.empty(remaining));
                } else {
                    entries.getLast().growAfter(remaining);
                }
                buffer.position(position + remaining);
                break;
            }
        }
        var block = new BlockDirEntity(length, entries);
        block.checkInvariant();
        return block;
    }

    DirEntry add(Inode.Id inode, FileType fileType, String name) {
        DirEntry newEntry;
        if (isEmpty()) {
            newEntry = entries.getFirst();
            newEntry.init(inode, fileType, name);
        } else {
            var index = findByAvailableSpace(name);
            newEntry = entries.get(index).split(inode, fileType, name);
            entries.add(index + 1, newEntry);
        }
        checkInvariant();
        return newEntry;
    }

    DirEntry get(String name) throws NoSuchFileException {
        return entries.get(findByName(name));
    }

    void rename(String name, String newName) throws NoSuchFileException {
        log.fine(() -> "Renaming entry [name=" + name + "]...");
        var index = findByName(name);
        log.finer(() -> "Found entry to rename [index=" + index + "]");
        var entry = entries.get(index);
        boolean renamed = entry.rename(newName);
        if (renamed) {
            return;
        }
        log.fine(() -> "Can't rename without increasing size, will relocate...");
        delete(index);
        add(entry.getInode(), entry.getFileType(), newName);
    }

    void delete(String name) throws NoSuchFileException {
        log.fine(() -> "Deleting entry [name=" + name + "]...");
        var index = findByName(name);
        log.finer(() -> "Found entry to delete [index=" + index + "]");
        delete(index);
    }

    void delete(int index) {
        var entry = entries.get(index);
        if (entries.size() == 1) {
            log.finer(() -> "Marking entry as empty...");
            entry.delete();
            checkInvariant();
            return;
        }
        if (index > 0) {
            log.finer(() -> "Increasing size of previous entry...");
            entries.get(index - 1).growAfter(entry.getLength());
        } else {
            log.finer(() -> "Increasing size of next entry...");
            entries.get(index + 1).growBefore(entry.getLength());
        }
        entries.remove(index);
        checkInvariant();
        log.fine(() -> "Successfully deleted entry [index=" + index + "]");
    }

    int findByName(String name) throws NoSuchFileException {
        return IntStream.range(0, entries.size())
                .filter(i -> {
                    DirEntry entry = entries.get(i);
                    return !entry.isEmpty() && entry.getName().equals(name);
                })
                .findAny()
                .orElseThrow(() -> new NoSuchFileException("File [" + name + "] was not found"));
    }

    int findByAvailableSpace(String newName) {
        return IntStream.range(0, entries.size())
                .filter(i -> entries.get(i).canBeSplit(newName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Not enough space"));
    }

    /**
     * Check that has only 1 empty entry.
     *
     * @return true if contains only 1 entry which is empty, false otherwise
     */
    boolean isEmpty() {
        return entries.size() == 1 && entries.getFirst().isEmpty();
    }

    void checkInvariant() {
        assert !entries.isEmpty() : "Entries list should not be empty";
        assert entries.stream().mapToInt(DirEntry::getLength).sum() == length : "Entries should occupy all bytes";
        assert !hasGaps() : "Entries should have no gaps between";
    }

    boolean hasGaps() {
        if (entries.size() == 1) {
            return false;
        }
        for (int i = 0; i < entries.size() - 1; i++) {
            DirEntry entry = entries.get(i);
            DirEntry next = entries.get(i + 1);
            if (entry.getPosition() + entry.getLength() != next.getPosition()) {
                return true;
            }
        }
        return false;
    }

    boolean isDirty() {
        return entries.stream().anyMatch(DirEntry::isDirty);
    }

    // -----------------------------------------------------------------------------------------------------------------

    List<DirEntry> getEntries() {
        return entries;
    }

}
