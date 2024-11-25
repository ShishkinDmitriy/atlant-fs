package org.atlantfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.logging.Logger;

class DirBlockMapping extends AbstractBlockMapping<DirEntryListBlock> implements DirectoryOperations {

    private static final Logger log = Logger.getLogger(DirBlockMapping.class.getName());

    DirBlockMapping(Inode inode) {
        super(inode);
    }

    static DirBlockMapping read(Inode inode, ByteBuffer buffer) {
        return AbstractBlockMapping.read(inode, buffer, DirBlockMapping::new);
    }

    static DirBlockMapping init(Inode inode, DirEntryList dirEntryList) throws BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        var result = new DirBlockMapping(inode);
        result.add(DirEntryListBlock.init(inode.getFileSystem(), dirEntryList));
        result.dirty = true;
        return result;
    }

    @Override
    public Iterator<DirEntry> iterator() {
        inode.ensureDirectory();
        return new Iterator<>() {

            int currentList = 0;
            Iterator<DirEntry> currentIterator = get(currentList).iterator();

            @Override
            public boolean hasNext() {
                var currentHasNext = currentIterator.hasNext();
                if (currentHasNext) {
                    return true;
                }
                if (currentList + 1 < inode.blocksCount()) {
                    currentList++;
                    currentIterator = get(currentList).iterator();
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
    public DirEntry add(Inode.Id inodeId, FileType fileType, String name) throws DirectoryOutOfMemoryException, BitmapRegionOutOfMemoryException, IndirectBlockOutOfMemoryException {
        inode.ensureDirectory();
        for (int i = 0; i < blocksCount(); i++) {
            try {
                var entryList = get(i);
                var add = entryList.add(inodeId, fileType, name);
                entryList.flush();
                return add;
            } catch (DirectoryOutOfMemoryException _) {
                // continue
            }
        }
        var entryList = DirEntryListBlock.init(fileSystem());
        var add = entryList.add(inodeId, fileType, name);
        add(entryList);
        entryList.flush();
        return add;
    }

    @Override
    public DirEntry get(String name) throws NoSuchFileException {
        inode.ensureDirectory();
        for (int i = 0; i < inode.blocksCount(); i++) {
            try {
                var entryList = get(i);
                return entryList.entryList().get(name);
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
                var entryList = get(i);
                entryList.entryList().rename(name, newName);
                entryList.flush();
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
                var entryList = get(i);
                entryList.entryList().delete(name);
                entryList.flush();
                return;
            } catch (NoSuchFileException _) {
                // continue
            }
        }
        throw new NoSuchFileException("File [" + name + "] was not found");
    }

    @Override
    public void delete() throws IOException {
        if (iterator().hasNext()) {
            throw new DirectoryNotEmptyException("Directory is not empty");
        }
        super.delete();
    }

    @Override
    DirEntryListBlock readBlock(Block.Id id) {
        return DirEntryListBlock.read(fileSystem(), id);
    }

}
