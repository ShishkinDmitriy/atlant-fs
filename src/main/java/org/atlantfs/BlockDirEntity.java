package org.atlantfs;

import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * Represent single block with a list of {@link DirEntry}.
 */
final class BlockDirEntity extends Block {

    private static final Logger log = Logger.getLogger(BlockDirEntity.class.getName());

    private final List<DirEntry> entries = new ArrayList<>();

    void read(ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            try {
                DirEntry dirEntry = DirEntry.read(buffer);
                entries.add(dirEntry);
            } catch (Exception e) {
                log.severe("");// TODO
                break;
            }
        }
    }

    DirEntry get(String name) throws NoSuchFileException {
        return entries.get(find(name));
    }

    void delete(String name) throws NoSuchFileException {
        int index = find(name);
        DirEntry entry = entries.get(index);
        DirEntry next = entries.size() > index + 1 ? entries.get(index + 1) : null;
        if (index == 0) {
            entry.setInode(Inode.NULL);
            entry.setDirty(true);
            if (next != null && next.isEmpty()) {
                entry.merge(next);
                entries.remove(index + 1);
            }
        } else {
            DirEntry previous = entries.get(index - 1);
            previous.setDirty(true);
            if (next != null && next.isEmpty()) {
                previous.merge(entry, next);
                entries.remove(index + 1);
                entries.remove(index);
            } else {
                previous.merge(entry);
                entries.remove(index);
            }
        }
    }

    public int find(String name) throws NoSuchFileException {
        return IntStream.range(0, entries.size())
                .filter(i -> {
                    DirEntry entry = entries.get(i);
                    return !entry.isEmpty() && entry.getName().equals(name);
                })
                .findAny()
                .orElseThrow(() -> new NoSuchFileException("File [" + name + "] was not found"));
    }

    public List<DirEntry> getEntries() {
        return entries;
    }

}
