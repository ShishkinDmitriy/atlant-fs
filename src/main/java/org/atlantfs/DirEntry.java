package org.atlantfs;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Represent single directory entry.
 * <p>
 * Inspired by {@code ext4_dir_entry_2}.
 * <pre>
 *          name length  file type
 *                    |  |
 *  inode       len   v  v  name     padding
 * +-----------+-----+--+--+--------+--------------+
 * |40 E2 01 00|10 00|03|01|64 69 72|00 00 00 00 00|
 * +-----------+-----+--+--+--------+--------------+
 *  F1 FB 09 00 10 00 04 01 64 69 72 32 00 00 00 00
 * </pre>
 * <ol>
 * <li>Inode number - 4 bytes
 * <li>Dir entry length - 2 bytes
 * <li>Name length - 1 bytes
 * <li>File type - 1 bytes
 * <li>Name - N bytes
 * <li>Padding - N bytes
 * </ol>
 * <p>
 * Max length of entry is {@code 4 + 2 + 1 + 1 + 255 = 263}.
 *
 * @see <a href="https://blogs.oracle.com/linux/post/understanding-ext4-disk-layout-part-2">Understanding Ext4 Disk Layout, Part 2</a>
 * @see <a href="https://blogs.oracle.com/linux/post/space-management-with-large-directories-in-ext4">Space Management With Large Directories in Ext4</a>
 */
class DirEntry {

    private static final Logger log = Logger.getLogger(DirEntry.class.getName());

    /**
     * The length of {@code inode}, {@code length}, {@code nameLength}, {@code fileType} fields.
     * <p>
     * Valid entry (with non-empty name) should be strictly greater than this length.
     */
    static final short ENTRY_MIN_LENGTH = Inode.Id.LENGTH + 2 + 1 + FileType.LENGTH;

    /**
     * Maximum length of name.
     * <p>
     * Limited by 1 byte of storage.
     */
    static final short NAME_MAX_LENGTH = 255;

    /**
     * The number of bytes to align whole entry.
     */
    static final short ALIGNMENT = 8;

    /**
     * Name used for entry with NULL inode.
     */
    static final String DEFAULT_NAME = "";

    /**
     * Position in block.
     * <p>
     * Value is not persisted.
     */
    private transient int position;

    /**
     * Dir entry length.
     */
    private short length;

    /**
     * Inode number.
     */
    private Inode.Id inode;

    /**
     * File type.
     */
    private FileType fileType;

    /**
     * File name.
     */
    private String name;

    /**
     * Flag indicating that Dir entry should be written on disk.
     * <p>
     * Value is not persisted.
     */
    private transient boolean dirty;

    private DirEntry(int position, short length, Inode.Id inode, FileType fileType, String name) {
        this.inode = inode;
        this.length = length;
        this.fileType = fileType;
        this.name = name;
        this.position = position;
        checkInvariant();
    }

    static DirEntry create(int position, Inode.Id inode, FileType fileType, String name) {
        return new DirEntry(position, aligned(name), inode, fileType, name);
    }

    static DirEntry create(int position, short length, Inode.Id inode, FileType fileType, String name) {
        return new DirEntry(position, length, inode, fileType, name);
    }

    static DirEntry empty(short length) {
        var entry = new DirEntry(0, length, Inode.Id.NULL, FileType.UNKNOWN, DEFAULT_NAME);
        entry.dirty = true;
        return entry;
    }

    /**
     * Read single Dir entry.
     * <p>
     * In case of any error buffer's position move back to initial position. Calling class
     * should decide how to handle this situation.
     * <p>
     * In case of zero inode name field will be ignored and empty string will be used
     * to keep value non-null but with zero length.
     *
     * @param buffer the byte buffer to read from
     * @return newly created Dir entry read from buffer
     * @throws IllegalArgumentException when length is less than minimum for this record
     * @throws BufferUnderflowException when buffer has fewer than required bytes
     */
    static DirEntry read(ByteBuffer buffer) {
        var initial = buffer.position();
        try {
            log.fine(() -> "Reading Dir entry [position=" + initial + "]...");
            var inode = Inode.Id.of(buffer.getInt());
            var length = buffer.getShort();
            if (length <= ENTRY_MIN_LENGTH) {
                throw new IllegalArgumentException("Too small Dir entry [length=" + length + "]");
            }
            FileType fileType;
            String name;
            if (inode.equals(Inode.Id.NULL)) {
                fileType = FileType.UNKNOWN;
                name = DEFAULT_NAME;
            } else {
                var nameLength = buffer.get();
                fileType = FileType.read(buffer);
                var chars = new byte[nameLength];
                buffer.get(chars);
                name = new String(chars);
            }
            buffer.position(initial + length);
            var entry = new DirEntry(initial, length, inode, fileType, name);
            log.finer(() -> "Successfully read Dir entry [entry=" + entry + "]");
            return entry;
        } catch (Exception e) {
            buffer.position(initial);
            throw e;
        }
    }

    void flush(ByteBuffer buffer) {
        var initial = buffer.position();
        log.fine(() -> "Writing Dir entry [position=" + initial + "]...");
        buffer.putInt(inode.value());
        buffer.putShort(length);
        buffer.put((byte) name.length());
        fileType.write(buffer);
        buffer.put(name.getBytes());
        buffer.position(initial + length);
        log.finer(() -> "Successfully written Dir entry [entry=" + this + "]");
        dirty = false;
    }

    boolean rename(String newName) {
        if (newName.length() > NAME_MAX_LENGTH) {
            throw new IllegalArgumentException("Name is too long");
        }
        if (newName.length() > name.length() + padding()) {
            return false;
        }
        name = newName;
        dirty = true;
        checkInvariant();
        return true;
    }

    DirEntry split(Inode.Id anotherInode, FileType anotherFileType, String anotherName) {
        if (isEmpty()) {
            throw new IllegalStateException("Can't split empty entry, use init method instead");
        }
        if (!canBeSplit(anotherName)) {
            throw new IllegalArgumentException("Not enough space to split");
        }
        var oldLength = length;
        length = aligned(name);
        var another = new DirEntry(position + length, (short) (oldLength - length), anotherInode, anotherFileType, anotherName);
        another.dirty = true;
        dirty = true;
        another.checkInvariant();
        checkInvariant();
        return another;
    }

    /**
     * Check that the entry has enough space for another entry if split this one.
     *
     * @param anotherName the name of another entry
     * @return true if it's enough space to split, false otherwise
     * @see #canBeSplit(short)
     */
    boolean canBeSplit(String anotherName) {
        return canBeSplit((short) anotherName.length());
    }

    /**
     * Check that the entry has enough space for another entry if split this one.
     *
     * @param anotherNameLength the length of the name of another entry
     * @return true if it's enough space to split, false otherwise
     * @see #canBeSplit(String)
     */
    boolean canBeSplit(short anotherNameLength) {
        return length - aligned(name) >= aligned(anotherNameLength);
    }

    /**
     * Called when entry before is deleted and need to occupy their space.
     * Will grow before up to the start previous entry.
     * <p>
     * Will mark entry as dirty.
     *
     * @param size number of bytes to grow to left
     */
    void growBefore(short size) {
        position -= size;
        growAfter(size);
    }

    /**
     * Called when need to grow record after up to the end of block in case when new block will not fit.
     * <p>
     * Will mark entry as dirty.
     *
     * @param size number of bytes to grow to right
     */
    void growAfter(short size) {
        length += size;
        dirty = true;
        checkInvariant();
    }

    void init(Inode.Id anotherInode, FileType anotherFileType, String anotherName) throws DirList.NotEnoughSpaceException {
        if (!isEmpty()) {
            throw new IllegalStateException("Dir entry already initialized");
        }
        if (length < aligned(anotherName)) {
            throw new DirList.NotEnoughSpaceException("Directory name [name=" + name + "] doesn't fit into [" + aligned(name) + "] bytes");
        }
        inode = anotherInode;
        fileType = anotherFileType;
        name = anotherName;
        dirty = true;
        checkInvariant();
    }

    /**
     * Called when it's single record in block, and it should be deleted.
     * <p>
     * Will mark entry as dirty.
     *
     * @see #init(Inode.Id, FileType, String)
     */
    void delete() {
        if (isEmpty()) { // To prevent dirty on already deleted
            return;
        }
        inode = Inode.Id.NULL;
        name = DEFAULT_NAME;
        dirty = true;
        checkInvariant();
    }

    /**
     * Calculate padding of the Dir entry.
     *
     * @return padding of the Dir entry
     */
    short padding() {
        return (short) (length - ENTRY_MIN_LENGTH - name.length());
    }

    void checkInvariant() {
        assert length >= aligned(name) : "Length [" + length + "] should be not less than actual data length [" + ENTRY_MIN_LENGTH + " + " + name.length() + "]";
        assert length % ALIGNMENT == 0 : "Length [" + length + "] should be aligned by [" + ALIGNMENT + "]";
        assert fileType != null : "File type should be specified";
        assert name.length() <= NAME_MAX_LENGTH : "Name should be [" + NAME_MAX_LENGTH + "] symbols max";
        assert inode != Inode.Id.NULL || name.isEmpty() : "Name should be empty for empty record";
    }

    static short aligned(String name) {
        return aligned((short) name.length());
    }

    static short aligned(short nameSize) {
        var overall = ENTRY_MIN_LENGTH + nameSize;
        var padding = (ALIGNMENT - (overall % ALIGNMENT)) % ALIGNMENT;
        return (short) (overall + padding);
    }

    // -----------------------------------------------------------------------------------------------------------------

    boolean isEmpty() {
        return inode.equals(Inode.Id.NULL);
    }

    String getName() {
        return name;
    }

    Inode.Id getInode() {
        return inode;
    }

    short getLength() {
        return length;
    }

    FileType getFileType() {
        return fileType;
    }

    int getPosition() {
        return position;
    }

    boolean isDirty() {
        return dirty;
    }

    @Override
    public String toString() {
        return "DirEntry{" +
                "inode=" + inode +
                ", length=" + length +
                ", fileType=" + fileType +
                ", name='" + name + '\'' +
                ", position=" + position +
                ", dirty=" + dirty +
                '}';
    }

}
